package com.ice.dis.stmp;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
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
	/** H2N上的网络消息(基于STMP-PROTOBUF的RPC)注册. */
	private final HashMap<String/* MSG */, RpcStub> rpcStubs = new HashMap<>();
	/** 缓存发出请求, 但为收到响应的事务(BEGIN). */
	public final Map<Integer/* STID. */, StmpInitiativeTrans<StmpH2N>> trans = new HashMap<>();
	/** 要连接的远端地址. */
	public InetSocketAddress addr;
	/** 是否还需要发送PING, 当发送一个PING后且未收到PONG时, 此标记+1. 当收到任何一个报文时清零. */
	private int ping = 0;
	/** 连接已建立. */
	private static final boolean ESTB = true;
	/** 连接已失去. */
	private static final boolean DISC = false;
	/** 是否第一次发起连接. */
	private boolean needWait = true;
	/** 服务订阅事务缓存. */
	private final HashMap<String /* DNE + msg */, Object /* Consumer<StmpSubscribeTrans<Message>> */> subscribes = new HashMap<>();
	/** 是否接收报文. */
	private boolean rxEnable = true;
	/** 用于加密通信的TOKEN. */
	public String token = null;

	public StmpH2N(InetSocketAddress addr)
	{
		super(ActorType.H2N, null, ByteBuffer.allocate(Cfg.libtsc_peer_mtu));
		this.protocol = ActorNet.STMP;/* 主动发起的连接只支持STMP. */
		this.wk = Tsc.hashWorkerIndex(this.hashCode());/* 随机选择一个线程. */
		this.addr = addr;
		this.peer = Net.getAddr(addr);
		this.enableSndBufAppCache();/* StmpH2N几乎总是被用于内部RPC, 因此总是安全的, 不存在被恶意堵塞套接字的场景. */
		this.getTworker().h2ns.add(this);
	}

	/** 连接已建立. */
	public abstract void est();

	/** 连接已失去. */
	public abstract void disc();

	/** 注册RPC. */
	public abstract boolean regRpc();

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/** 消息处理. */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** 连接断开事件. */
	public void evnDis()
	{
		this.status.set(StmpH2N.DISC);
		this.ping = 0;
		this.continues.clear();
		this.rbb.clear();
		this.trans.clear();
		this.subscribes.clear();
		if (this.wbb != null)
		{
			this.wbb.clear();
		}
		this.disc();/* 连接已失去. */
		if (this.rxEnable)
			this.connect();/* 重连. */
	}

	public boolean evnMsg(StmpNode root)
	{
		if (!this.rxEnable)
			return true;
		this.ping = 0;
		switch (root.self.t)
		{
		case Stmp.STMP_TAG_TRANS_BEGIN:/* 事务开始. */
			return this.stmp_begin(root);
		case Stmp.STMP_TAG_TRANS_CONTINUE:/* 事务继续. */
			return this.stmp_continue(root);
		case Stmp.STMP_TAG_TRANS_END: /* 事务结束. */
			return this.stmp_end(root);
		default:
			return false;
		}
	}

	/** 事务开始. */
	private final boolean stmp_begin(StmpNode root)
	{
		Integer tid = StmpDec.getInt(root, Stmp.STMP_TAG_DTID);
		if (tid == null)
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
		if (contin != null)
		{/* 后面还有continue. */
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_DAT");
				return false;
			}
			this.continues.put(tid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_BEGIN, tid, msg, dat, null, null, Byte.MIN_VALUE, Short.MIN_VALUE));
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		if (dat == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DAT");
			return false;
		}
		return this.invokeBegin(stub, dat, tid);
	}

	public final boolean stmp_continue(StmpNode root)
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
				Log.debug("can not found StmpPduCache for this stid, may be it was timeout. stid: %08X", stid);
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
		// TODO switch
		RpcStub stub = Tsc.rpcStubs.get(scc.msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, peer: %s", scc.msg, this.peer);
			return false;
		}
		if (scc.trans == Stmp.STMP_TAG_TRANS_BEGIN)
			return this.invokeBegin(stub, scc.bytes(), scc.stid);
		if (scc.trans == Stmp.STMP_TAG_TRANS_END)
			return this.invokeEnd(stub, scc.bytes(), scc.stid);
		return false;
	}

	/** 事务开始. */
	private final boolean invokeBegin(RpcStub stub, byte[] dat, int stid)
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
			stub.cb.invoke(null, this, new StmpPassiveTrans<StmpH2N>(this, stid, begin), begin);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("exception occur on rpcStub: %s, exception: %s", stub, Log.trace(e));
			return false;
		}
		return true;
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
		if (tt == null) /* 找不到事务, 可能已经超时. 直接抛弃, 不将此事件上报应用. */
		{
			if (Log.isDebug())
				Log.debug("can not found Tn2htrans for tid: %08X, may be it was timeout.", tid);
			return true;
		}
		RpcStub stub = this.rpcStubs.get(tt.begin.getClass().getName());
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, peer: %s", tt.begin.getClass().getName(), this.peer);
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
		tt.ret = ret.intValue(); /* 返回值已到达. */
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
				return false;
			} catch (InvocationTargetException e)
			{
				if (Log.isDebug())
					Log.debug("%s,", Log.trace(e.getTargetException()));
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

	public final void begin(Message begin, Consumer<TstmpEnd> endCb, Consumer<StmpInitiativeTrans<StmpH2N>> tm, int sec/* 超时时间(秒). */)
	{
		super.future(x ->
		{
			StmpInitiativeTrans<StmpH2N> tt = new StmpInitiativeTrans<>(++this.tid, this, begin, endCb, tm, sec);
			this.trans.put(this.tid, tt);
			if (!this.status.get()) /* 连接还未建立. */
				return;
			super.sendBegin(this.tid, begin);
		});
	}

	/** 尝试连接到远端服务器. */
	public final void connect()
	{
		StmpH2N h2n = this;
		new Thread(() ->
		{
			if (h2n.needWait)/* 第一次连接,无需等待. */
				h2n.needWait = false;
			else
				Misc.sleep(Cfg.libtsc_h2n_reconn * 1000);
			try
			{
				SocketChannel sc = SocketChannel.open(h2n.addr);
				h2n.future(x ->
				{
					if (this.regSocketChannel(sc))
					{
						Log.info("connect to remote server successfully, addr: %s", Net.getAddr(h2n.addr));
						h2n.sc = sc;
						h2n.est = true;
						h2n.lts = Tsc.clock;
						if (!h2n.status.compareAndSet(StmpH2N.DISC, StmpH2N.ESTB))
							Log.fault("it`s a bug: %s", h2n.status.get());
						h2n.est();
					} else
					{
						if (Log.isWarn())
							Log.warn("can not connect to remote-addr: %s", Net.getAddr(h2n.addr));
						Net.closeSocketChannel(sc);
						h2n.connect();
					}
				});
			} catch (IOException e)
			{
				if (Log.isWarn())
					Log.warn("can not connect to remote-addr(%s): %s", h2n.name, Net.getAddr(h2n.addr));
				if (Log.isTrace())
					Log.trace("%s", Log.trace(e));
				h2n.connect();/* 尝试重试连接. */
			}
		}).start();
	}

	/** 将SocketChannel注册的当前线程. */
	private final boolean regSocketChannel(SocketChannel sc)
	{
		try
		{
			this.getTworker().setSocketOpt(sc);
			sc.register(this.getTworker().slt, SelectionKey.OP_READ);
			this.getTworker().ans.put(sc, this);
			return true;
		} catch (Exception e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return false;
		}
	}

	/** 超时处理. */
	private final void timeoutTrans(StmpInitiativeTrans<StmpH2N> trans)
	{
		trans.tm = true;
		Misc.exeConsumer(trans.tmCb, trans);
	}

	/** H2N上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于H2N主动发起的事务. */
	public final boolean regBeginEnd(Class<?> begin, Class<?> end)
	{
		return this.regMsg(begin, end, null, null, false, false, null);
	}

	/** H2N上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于H2N被动接收的事务. */
	public final boolean regBeginEnd(Class<?> begin, Class<?> end, Class<?> handler)
	{
		return this.regMsg(begin, end, null, handler, false, false, null);
	}

	/** H2N上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于对外的RPC调用. */
	public final boolean regRPC(Class<?> begin, Class<?> end, Class<?> handler)
	{
		return this.regMsg(begin, end, null, handler, true, false, null);
	}

	/** H2N上的网络消息(基于STMP-PROTOBUF的RPC)注册. */
	private final boolean regMsg(Class<?> begin, Class<?> end, Class<?> uni, Class<?> handler, boolean rpc, boolean service, String srvDoc)
	{
		Class<?> msg = uni == null ? begin : uni; /* 为UNI消息时, 没有BEGIN, 使用UNI的className. */
		if (this.rpcStubs.get(msg.getName()) != null)
		{
			if (Log.isError())
				Log.error("duplicate msg: %s", msg.getName());
			return false;
		}
		Method m = null;
		if (handler != null)
		{
			String sname = msg.getSimpleName();
			m = Misc.findMethodByName(handler, sname.substring(0, 1).toLowerCase() + sname.substring(1, sname.length()));
			if (m == null)
			{
				Log.error("can not found call back method for msg: %s, handler: %s", msg.getName(), handler.getName());
				return false;
			}
		}
		if (Log.isTrace())
			Log.trace("H2N-RPC-STUB, msg: %s, handler: %s, begin: %s, end: %s, uni: %s, service: %s", msg.getName(), handler == null ? "NULL" : handler.getName(), //
					begin == null ? "NULL" : begin.getName(), //
					end == null ? "NULL" : end.getName(), //
					uni == null ? "NULL" : uni.getName(), //
					service ? "true" : "false");
		System.out.println("msg:" + msg.getName());
		this.rpcStubs.put(msg.getName(), new RpcStub(m, begin, end, uni, true, rpc, service, srvDoc));
		return true;
	}

	/** 设置用于加密通信的TOKEN. */
	public final void setToken(String token)
	{
		this.token = token;
	}
}
