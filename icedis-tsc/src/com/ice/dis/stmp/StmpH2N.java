package com.ice.dis.stmp;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.cfg.RpcStub;
import com.ice.dis.core.Tsc;

import misc.Log;
import misc.Misc;
import misc.Net;
import stmp.Stmp;
import stmp.StmpDec;
import stmp.StmpNode;

public abstract class StmpH2N extends StmpNet
{
	/** H2N的网络状态. */
	public AtomicBoolean status = new AtomicBoolean(StmpH2N.DISC);
	/** 缓存发出了请求, 但未收到响应的事务(BEGIN). */
	private final HashMap<Integer, StmpInitiativeTrans<StmpH2N>> trans = new HashMap<>();
	/** 缓存发出了请求, 但未收到响应的事务(SWITCH). */
	private final HashMap<Integer /* STID. */, StmpSwitchInitiativeTrans<StmpH2N>> switchTrans = new HashMap<>();
	/** H2N上的网络消息(基于STMP-PROTOBUF的RPC)注册. */
	private final HashMap<String/* MSG */, RpcStub> rpcStubs = new HashMap<>();
	/** 要连接的远端地址. */
	private InetSocketAddress addr = null;
	/** 是否还需要发送PING, 当发送一个PING后且未收到PONG时, 此标记+1. 当收到任何一个报文时清零. */
	private int ping = 0;
	/** 是否第一次发起连接. */
	private boolean needWait = true;
	/** 是否接收报文. */
	private boolean rxEnable = true;
	/** 连接已建立. */
	private static final boolean ESTB = true;
	/** 连接已失去. */
	private static final boolean DISC = false;
	/** 服务订阅事务缓存. */
	private final HashMap<String /* DNE + msg */, Object /* Consumer<StmpSubscribeTrans<Message>> */> subscribes = new HashMap<>();
	/** 用于加密token. */
	public String token = null;

	public StmpH2N(InetSocketAddress addr)
	{
		super(ActorType.H2N, null, ByteBuffer.allocate(Cfg.libtsc_peer_mtu));
		this.protocol = ActorNet.STMP_PROTOCOL;
		this.wk = Tsc.hashWorkerIndex(this.hashCode());
		this.addr = addr;
		this.enableSndBufAppCache();
		this.getTworker().h2ns.add(this);
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** 连接已建立. */
	public abstract void estb();

	/** 连接已失去. */
	public abstract void disc();

	/** H2N连接上RPC注册. */
	public abstract boolean regRpc();

	/** 网络消息送达时间(STMP). */
	public boolean evnMsg(StmpNode root)
	{
		if (!this.rxEnable)
			return false;
		this.ping = 0; /* 清空ping标志. */
		switch (root.self.t)
		{
		case Stmp.STMP_TAG_TRANS_BEGIN:/* 事务开始. */
			return this.stmp_begin(root);
		case Stmp.STMP_TAG_TRANS_CONTINUE:/* 事务继续. */
			return this.stmp_continue(root);
		case Stmp.STMP_TAG_TRANS_END:/* 事务结束. */
			return this.stmp_end(root);
		case Stmp.STMP_TAG_TRANS_UNI: /* 单向消息. */
			return this.stmp_uni(root);
		case Stmp.STMP_TAG_TRANS_SWITCH:
			return this.stmp_switch(root);
		case Stmp.STMP_TAG_TRANS_PONG: /* PONG. */
			return true;
		default:
			if (Log.isDebug())
				Log.debug("it`s an unexpected STMP message, tag: %02X, peer: %s, stack: %s", root.self.t, this.peer, Misc.getStackInfo());
			return false;
		}
	}

	/** 连接断开事件. */
	public void evnDis()
	{
		this.status.set(StmpH2N.DISC);
		this.continues.clear();
		this.ping = 0;
		this.rbb.clear();
		this.trans.clear();
		this.subscribes.clear();
		if (this.wbb != null)
			this.wbb.clear();
		this.disc();
		if (this.rxEnable)
			this.connect();
	}

	/** 尝试连接到远端服务器. */
	protected final void connect()
	{
		StmpH2N h2n = this;
		new Thread(() ->
		{
			if (this.needWait)/* 第一次连接不需要等待. */
				this.needWait = false;
			else
				Misc.sleep(Cfg.libtsc_h2n_reconn * 1000);
			//
			try
			{
				SocketChannel sc = SocketChannel.open(addr);
				h2n.future(x ->
				{
					if (h2n.regSocketCHannel(sc))
					{
						Log.info("connect to remote server successfully, addr: %s", Net.getAddr(h2n.addr));
						h2n.sc = sc;
						h2n.est = true;
						h2n.lts = Tsc.clock;
						if (!h2n.status.compareAndSet(StmpH2N.DISC, StmpH2N.ESTB))
							Log.fault("it`s a bug: %s", h2n.status.get());
						h2n.estb();
					}
				});
			} catch (IOException e)
			{
				if (Log.isWarn())
					Log.warn("can not connect to remote-addr(%s): %s", h2n.name, Net.getAddr(h2n.addr));
				if (Log.isTrace())
					Log.trace("%s", Log.trace(e));
				h2n.connect();
			}

		}).start();
	}

	/** 将SocketChannel注册到当前线程. */
	private final boolean regSocketCHannel(SocketChannel sc)
	{
		try
		{
			this.getTworker().setSocketOpt(sc);
			sc.register(this.getTworker().slt, SelectionKey.OP_READ);
			this.getTworker().ans.put(sc, this);
			return true;
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return false;
		}
	}

	/** 事务开始. */
	private final boolean stmp_begin(StmpNode root)
	{
		Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);
		if (stid == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_STID");
			return false;
		}
		String msg = StmpDec.getStr(root, Stmp.STMP_TAG_MSG);
		if (msg == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_MSG");
			return false;
		}
		RpcStub stub = this.rpcStubs.get(msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, cmd: %s, peer: %s", msg, this.peer);
			return false;
		}
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
		{
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_DAT");
				return false;
			}
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_BEGIN, stid, msg, dat, null, null, Byte.MIN_VALUE, Short.MIN_VALUE)); /* 缓存. */
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		if (dat == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DAT");
			return false;
		}
		return this.invokeBegin(stub, dat, stid);
	}

	/** 事务开始. */
	private final boolean invokeBegin(RpcStub stub, byte[] dat, int tid)
	{
		Message begin = null;
		try
		{
			begin = stub.newBeginMsg(dat);
		} catch (InvalidProtocolBufferException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		}
		try
		{
			stub.cb.invoke(null, new StmpPassiveTrans<StmpH2N>(this, tid, begin), begin);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		}
		return true;
	}

	/** 事务继续. */
	private final boolean stmp_continue(StmpNode root)
	{
		Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);
		if (stid == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_STID");
			return false;
		}
		StmpContinueCache scc = this.continues.get(stid);
		if (scc == null) /* 找不到对应的事务. */
		{
			if (Log.isDebug())
				Log.debug("can not found StmpPduCache for this stid: %08X, may be it was timeout, this: %s", stid, this);
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		if (dat == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DAT");
			return false;
		}
		scc.push(dat); /* 继续缓存. */
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
			return true;
		this.continues.remove(stid); /* 移除事务. */
		//
		if (scc.trans == Stmp.STMP_TAG_TRANS_SWITCH) /* SWITCH. */
			return this.switchContinue(scc);
		RpcStub stub = this.rpcStubs.get(scc.msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, this: %s", scc.msg, this);
			return false;
		}
		if (scc.trans == Stmp.STMP_TAG_TRANS_BEGIN)
			return this.invokeBegin(stub, scc.bytes(), scc.stid);
		if (scc.trans == Stmp.STMP_TAG_TRANS_UNI)
			return this.invokeUni(stub, scc.bytes());
		if (scc.trans == Stmp.STMP_TAG_TRANS_END)
			return this.invokeEnd(stub, scc.bytes(), scc.stid);
		else
			Log.fault("it`s a bug, STMP_TRANS: %02X, this: %s", scc.trans, this);
		return false;
	}

	/** 事务结束. */
	private final boolean stmp_end(StmpNode root)
	{
		Integer tid = StmpDec.getInt(root, Stmp.STMP_TAG_DTID);
		if (tid == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DTID");
			return false;
		}
		StmpInitiativeTrans<StmpH2N> tt = this.trans.get(tid);
		if (tt == null) /* 找不到事务, 可能已经超时. */
		{
			if (Log.isDebug())
				Log.debug("can not found transaction for tid: %08X, may be it was timeout, this: %s", tid, this);
			return true;
		}
		RpcStub stub = this.rpcStubs.get(tt.begin.getClass().getName());
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, this: %s", tt.begin.getClass().getName(), this);
			this.trans.remove(tid);
			this.timeoutTrans(tt);
			return false;
		}
		Short ret = StmpDec.getShort(root, Stmp.STMP_TAG_RET);
		if (ret == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_RET");
			this.trans.remove(tid);
			this.timeoutTrans(tt);
			return false;
		}
		tt.ret = ret.intValue();
		//
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
		{
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_DAT");
				this.trans.remove(tid);
				this.timeoutTrans(tt);
				return false;
			}
			this.continues.put(tid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_END, tid, tt.begin.getClass().getName(), dat, null, null, Byte.MIN_VALUE, Short.MIN_VALUE)); /* 缓存. */
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		return this.invokeEnd(stub, dat, tid);
	}

	/** 单向消息. */
	private final boolean stmp_uni(StmpNode root)
	{
		String msg = StmpDec.getStr(root, Stmp.STMP_TAG_MSG);
		if (msg == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_MSG");
			return false;
		}
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
		{
			Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);
			if (stid == null) /* 有continue时, 必需要有源事务ID. */
			{
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_STID");
				return false;
			}
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_DAT");
				return false;
			}
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_UNI, stid, msg, dat, null, null, Byte.MIN_VALUE, Short.MIN_VALUE)); /* 缓存. */
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		if (dat == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DAT");
			return false;
		}
		RpcStub stub = this.rpcStubs.get(msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, this: %s", msg, this);
			return false;
		}
		return this.invokeUni(stub, dat);
	}

	private final boolean switchContinue(StmpContinueCache scc)
	{
		if (scc.tag == Stmp.STMP_TAG_TRANS_BEGIN)
			return this.switchContinueBegin(scc);
		if (scc.tag == Stmp.STMP_TAG_TRANS_END)
			return this.switchContinueEnd(scc);
		if (Log.isWarn())
			Log.warn("it`s an unexpected tag: %02X, this: %s", scc.tag, this);
		return true;
	}

	private final boolean switchContinueEnd(StmpContinueCache scc)
	{
		StmpSwitchInitiativeTrans<StmpH2N> trans = this.switchTrans.get(scc.stid);
		if (trans == null)
		{
			if (Log.isDebug())
				Log.debug("can not found transaction for stid: %08X, may be it was timeout, this: %s", scc.stid, this);
			return true;
		}
		RpcStub stub = this.rpcStubs.get(trans.begin.getClass().getName());
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, this: %s", trans.begin.getClass().getName(), this);
			return true;
		}
		this.switchTrans.remove(scc.stid);
		this.invokeSwitchEnd(scc.sne, scc.dne, scc.stid, scc.bytes(), stub, trans);
		return true;
	}

	/** 单向消息. */
	private final boolean invokeUni(RpcStub stub, byte[] dat)
	{
		Message uni = null;
		try
		{
			uni = stub.newUniMsg(dat);
		} catch (InvalidProtocolBufferException | IllegalAccessException | IllegalArgumentException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		} catch (InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e.getTargetException()));
			return false;
		}
		try
		{
			stub.cb.invoke(null, this, uni);
		} catch (IllegalAccessException | IllegalArgumentException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		} catch (InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("exception occur on rpcStub: %s, exception: %s", stub, Log.trace(e.getTargetException()));
			return false;
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("exception occur on rpcStub: %s, exception: %s", stub, Log.trace(e));
			return false;
		}
		Tsc.log.receivedUni(this, uni);
		return true;
	}

	/** 事务前转. */
	private final boolean stmp_switch(StmpNode root)
	{
		Byte tag = StmpDec.getByte(root, Stmp.STMP_TAG_TAG);
		if (tag == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_TAG");
			return true;
		}
		Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);
		if (stid == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_STID");
			return true;
		}
		String sne = StmpDec.getStr(root, Stmp.STMP_TAG_SNE);
		if (sne == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_SNE");
			return true;
		}
		String dne = StmpDec.getStr(root, Stmp.STMP_TAG_DNE);
		if (dne == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DNE");
			return true;
		}
		if (!dne.equals(this.ne))
		{
			Log.fault("it`s a bug, message it not for this H2N, this: %s, dne: %s", this, dne);
			return true;
		}
		if (tag == Stmp.STMP_TAG_TRANS_BEGIN)
			this.switchBegin(root, stid, sne, dne);
		else if (tag == Stmp.STMP_TAG_TRANS_END)
			this.switchEnd(root, stid, sne, dne);
		else
		{
			if (Log.isWarn())
				Log.warn("unsupported operation, this: %s, sne: %s", this, dne);
		}
		return true;
	}

	/** 事务前转(END). */
	private final void invokeSwitchEnd(String sne, String dne, int stid, byte[] dat, RpcStub stub, StmpSwitchInitiativeTrans<StmpH2N> trans)
	{
		Message end = null;
		if (dat != null)
		{
			try
			{
				end = stub.newEndMsg(dat);
			} catch (InvalidProtocolBufferException | IllegalAccessException | IllegalArgumentException e)
			{
				if (Log.isDebug())
					Log.debug("this: %s, msg: %s, exception: %s", this, trans.begin.getClass().getName(), Log.trace(e));
				this.timeoutTransSwitch(trans);
				return;
			} catch (InvocationTargetException e)
			{
				if (Log.isDebug())
					Log.debug("this: %s, msg: %s, exception: %s", this, trans.begin.getClass().getName(), Log.trace(e.getTargetException()));
				this.timeoutTransSwitch(trans);
				return;
			} catch (Exception e)
			{
				if (Log.isDebug())
					Log.debug("this: %s, msg: %s, exception: %s", this, trans.begin.getClass().getName(), Log.trace(e));
				this.timeoutTransSwitch(trans);
				return;
			}
		}
		trans.end = new TstmpEnd(trans.ret, end);
		Misc.exeConsumer(trans.endCb, trans);
	}

	/** 事务前转(BEGIN). */
	private final void switchBegin(StmpNode root, int stid, String sne, String dne)
	{
		String msg = StmpDec.getStr(root, Stmp.STMP_TAG_MSG);
		if (msg == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_MSG");
			return;
		}
		RpcStub stub = this.rpcStubs.get(msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, this: %s", msg, this);
			return;
		}
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
		{
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_DAT");
				return;
			}
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_SWITCH, stid, msg, dat, sne, dne, Stmp.STMP_TAG_TRANS_BEGIN, Short.MIN_VALUE)); /* 缓存. */
			return;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		if (dat == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DAT");
			return;
		}
		this.invokeSwitchBegin(sne, dne, stid, msg, dat, stub);
	}

	/** 事务前转(END). */
	private final void switchEnd(StmpNode root, int stid, String sne, String dne)
	{
		Short ret = StmpDec.getShort(root, Stmp.STMP_TAG_RET);
		if (ret == null)
		{
			if (Log.isWarn())
				Log.warn("missing required field: STMP_TAG_RET, sne: %s, dne: %s, root: %s", sne, dne, StmpDec.printNode2Str(root));
			this.switchTrans.remove(stid);
			return;
		}
		StmpSwitchInitiativeTrans<StmpH2N> trans = this.switchTrans.get(stid);
		if (trans == null)
		{
			if (Log.isWarn())
				Log.warn("can not found transaction for stid: %08X, may be it was timeout, this: %s", stid, this);
			return;
		}
		trans.ret = ret.intValue();
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
		{
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isWarn())
					Log.warn("missing required field: STMP_TAG_DAT, sne: %s, dne: %s, root: %s", sne, dne, StmpDec.printNode2Str(root));
				return;
			}
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_SWITCH, stid, trans.begin.getClass().getName(), dat, sne, dne, Stmp.STMP_TAG_TRANS_END, ret)); /* 缓存. */
			return;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		RpcStub stub = this.rpcStubs.get(trans.begin.getClass().getName());
		if (stub == null)
		{
			if (Log.isWarn())
				Log.warn("unsupported operation, msg: %s, this: %s", trans.begin.getClass().getName(), this);
			return;
		}
		this.switchTrans.remove(stid);
		this.invokeSwitchEnd(sne, dne, stid, dat, stub, trans);
	}

	/** 事务前转(BEGIN). */
	private final void invokeSwitchBegin(String sne, String dne, int stid, String msg, byte[] dat, RpcStub stub)
	{
		Message begin = null;
		try
		{
			begin = stub.newBeginMsg(dat);
		} catch (InvalidProtocolBufferException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("this: %s, msg: %s, e: %s", this, msg, Log.trace(e));
			return;
		}
		if (stub.service)
		{
			this.invokeSwitchBeginService(sne, dne, stid, msg, dat, stub, begin);
			return;
		}
		try
		{
			stub.cb.invoke(null, this, new StmpSwitchPassiveTrans(this, stid, begin, sne, dne), begin);
		} catch (IllegalAccessException | IllegalArgumentException e)
		{
			if (Log.isDebug())
				Log.debug("this: %s, msg: %s, e: %s", this, msg, Log.trace(e));
			return;
		} catch (InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("exception occur on msg: %s, this: %s, exception: %s", msg, this, Log.trace(e.getTargetException()));
			return;
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("exception occur on msg: %s, this: %s, exception: %s", msg, this, Log.trace(e));
			return;
		}
	}

	/** 事务开始(收到一个订阅服务所发布的消息). */
	private final void invokeSwitchBeginService(String sne, String dne, int stid, String msg, byte[] dat, RpcStub stub, Message begin)
	{
		try
		{
			@SuppressWarnings("unchecked")
			Consumer<StmpServiceTrans<Message>> con = (Consumer<StmpServiceTrans<Message>>) this.subscribes.get(sne + msg);
			if (con == null)
			{
				if (Log.isWarn())
					Log.warn("it`s an unexpected service message: %s, this: %s", msg, this);
				return;
			}
			con.accept(new StmpServiceTrans<>(sne, this, begin, stid));
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return;
		}
	}

	/** 事务结束. */
	private final boolean invokeEnd(RpcStub stub, byte dat[], int tid)
	{
		StmpInitiativeTrans<StmpH2N> tt = this.trans.remove(tid);
		Message end = null;
		if (dat != null)
		{
			try
			{
				end = stub.newEndMsg(dat);
			} catch (InvalidProtocolBufferException | IllegalAccessException | IllegalArgumentException e)
			{
				if (Log.isDebug())
					Log.debug("%s,", Log.trace(e));
				this.timeoutTrans(tt);
				return false;
			} catch (InvocationTargetException e)
			{
				if (Log.isDebug())
					Log.debug("%s,", Log.trace(e.getTargetException()));
				this.timeoutTrans(tt);
				return false;
			} catch (Exception e)
			{
				if (Log.isDebug())
					Log.debug("%s,", Log.trace(e));
				this.timeoutTrans(tt);
				return false;
			}
		}
		tt.end = new TstmpEnd(tt.ret, end);
		Misc.exeConsumer(tt.endCb, tt.end);
		return true;
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** 不再接收报文. */
	public final void disableRx()
	{
		this.rxEnable = false;
	}

	/** 做两件事, 1, 检查超时的事务. 2, 决定是否要发心跳. */
	public final void quartz(long now)
	{
		this.checkTrans(now);
		this.checkSwitchTrans(now);
		this.checkHeartBeat(now);
	}

	/** 检查超时的主动事务. */
	private final void checkTrans(long now)
	{
		Iterator<StmpInitiativeTrans<StmpH2N>> it = this.trans.values().iterator();
		ArrayList<StmpInitiativeTrans<StmpH2N>> tmp = new ArrayList<>();
		while (it.hasNext())
		{
			StmpInitiativeTrans<StmpH2N> trans = it.next();
			if (now < trans.tmts)
				continue;
			if (Log.isDebug())
				Log.debug("have a H2N transaction timeout, tid: %08X, msg: %s", trans.tid, trans.begin.getClass().getName());
			tmp.add(trans);
			it.remove();
		}
		tmp.forEach(trans -> this.timeoutTrans(trans));
	}

	/** 检查超时的主动事务(SWITCH). */
	private final void checkSwitchTrans(long now)
	{
		Iterator<StmpSwitchInitiativeTrans<StmpH2N>> it = this.switchTrans.values().iterator();
		ArrayList<StmpSwitchInitiativeTrans<StmpH2N>> tmp = new ArrayList<>();
		while (it.hasNext())
		{
			StmpSwitchInitiativeTrans<StmpH2N> trans = it.next();
			if (now < trans.tmts)
				continue;
			if (Log.isDebug())
				Log.debug("have a H2N transaction timeout, tid: %08X, sne: %s, dne: %s, msg: %s", trans.tid, trans.sne, trans.dne, trans.begin.getClass().getName());
			tmp.add(trans);
			it.remove();
		}
		tmp.forEach(trans -> this.timeoutTransSwitch(trans));
	}

	/** 超时处理. */
	private final void timeoutTransSwitch(StmpSwitchInitiativeTrans<StmpH2N> trans)
	{
		trans.tm = true;
		Misc.exeConsumer(trans.tmCb, trans);
		Tsc.log.trans(trans);
	}

	/** 超时处理. */
	private final void timeoutTrans(StmpInitiativeTrans<StmpH2N> trans)
	{
		trans.tm = true;
		Misc.exeConsumer(trans.tmCb, trans);
		Tsc.log.trans(trans);
	}

	private final boolean switchContinueBegin(StmpContinueCache scc)
	{
		RpcStub stub = this.rpcStubs.get(scc.msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, this: %s", scc.msg, this);
			return true;
		}
		this.invokeSwitchBegin(scc.sne, scc.dne, scc.stid, scc.msg, scc.bytes(), stub);
		return true;
	}

	/** 检查是否要发送心跳. */
	private final void checkHeartBeat(long now)
	{
		if (!this.est) /* 连接还未建立. */
			return;
		if (this.ping == 0)
		{
			if (now - this.lts > Cfg.libtsc_h2n_heartbeat * 1000) /* 如果链路上超过LIBTSC_PEER_HEARTBEAT时间无任何消息, 才发送心跳. */
			{
				this.ping += 1;
				this.send(new byte[] { Stmp.STMP_TAG_TRANS_PING });
			}
			return;
		}
		if (this.ping == 3) /* 已经发送过两次心跳, 强制断开连接. */
		{
			this.close();
			return;
		}
		if (now - this.lts > Cfg.libtsc_h2n_heartbeat * 1000 * (this.ping + 1)) /* 每间隔一个LIBTSC_PEER_HEARTBEAT时间再发送心跳. */
		{
			this.ping += 1;
			this.send(new byte[] { Stmp.STMP_TAG_TRANS_PING });
		}
	}

}
