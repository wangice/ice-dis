package com.ice.dis.stmp;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ice.dis.actor.Tusr;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.cfg.RpcStub;
import com.ice.dis.core.Tsc;

import misc.Log;
import misc.Misc;
import stmp.Stmp;
import stmp.Stmp.Ret;
import stmp.StmpDec;
import stmp.StmpNode;

public class StmpN2H extends StmpNet
{
	/** 连接上的用户数据. */
	public Tusr<StmpN2H, StmpNode> tusr = null;
	/** 缓存发出了请求, 但未收到响应的事务(BEGIN). */
	public final HashMap<Integer, StmpInitiativeTrans<StmpN2H>> trans = new HashMap<>();
	/** 缓存了所有的, 有Tusr的StmpN2H. */
	public static final ConcurrentHashMap<String /* NE. */, StmpN2H> nes = new ConcurrentHashMap<>();

	public StmpN2H(SocketChannel sc, int wk)
	{
		super(ActorType.N2H, sc, ByteBuffer.allocate(Cfg.libtsc_peer_mtu));
		this.est = true;
		this.gts = Tsc.clock;
		this.wk = wk;
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** 事务开始. */
	public final void begin(Message begin, Consumer<TstmpEnd> endCb, Consumer<StmpInitiativeTrans<StmpN2H>> tmCb, int tm /* 超时时间(秒). */)
	{
		StmpInitiativeTrans<StmpN2H> tt = new StmpInitiativeTrans<StmpN2H>(++this.tid, this, begin, endCb, tmCb, tm);
		this.trans.put(tt.tid, tt); /* 总是缓存. */
		super.sendBegin(tt.tid, tt.begin);
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	public boolean evnMsg(StmpNode root)
	{
		if (this.tusr != null)
			this.tusr.evnTraffic(root);
		switch (root.self.t)
		{
		case Stmp.STMP_TAG_TRANS_BEGIN: /* 事务开始. */
			return this.stmp_begin(root);
		case Stmp.STMP_TAG_TRANS_CONTINUE: /* 事务开始. */
			return this.stmp_continue(root);
		case Stmp.STMP_TAG_TRANS_END: /* 事务结束. */
			return this.stmp_end(root);
		case Stmp.STMP_TAG_TRANS_UNI: /* 单向消息. */
			return this.stmp_uni(root);
		case Stmp.STMP_TAG_TRANS_SWITCH: /* 事务前转. */
			return this.stmp_switch(root);
		case Stmp.STMP_TAG_TRANS_PING: /* PING. */
			return this.stmp_ping(root);
		default:
			if (Log.isDebug())
				Log.debug("it`s an unexpected STMP message, tag: %02X, peer: %s, root: %s, stack: %s", root.self.t, this.peer, StmpDec.printNode2Str(root), Misc.getStackInfo());
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
		RpcStub stub = Tsc.rpcStubs.get(msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, peer: %s", msg, this.peer);
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
		//
		if (scc.trans == Stmp.STMP_TAG_TRANS_SWITCH) /* SWITCH. */
			return this.invokeSwitch(scc);
		RpcStub stub = Tsc.rpcStubs.get(scc.msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, peer: %s", scc.msg, this.peer);
			return false;
		}
		if (scc.trans == Stmp.STMP_TAG_TRANS_BEGIN)
			return this.invokeBegin(stub, scc.bytes(), scc.stid);
		if (scc.trans == Stmp.STMP_TAG_TRANS_UNI)
			return this.invokeUni(stub, scc.bytes());
		if (scc.trans == Stmp.STMP_TAG_TRANS_END)
			return this.invokeEnd(stub, scc.bytes(), scc.stid);
		else
			Log.fault("it`s a bug, STMP_TRANS: %02X", scc.trans);
		return false;
	}

	/** 事务开始. */
	private final boolean invokeBegin(RpcStub stub, byte dat[], int stid)
	{
		Message begin = null;
		try
		{
			begin = stub.newBeginMsg(dat);
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
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		}
		if ((this.tusr == null && stub.tusr) || (this.tusr != null && !stub.tusr)) /* 时机不对. */
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, stub: %s, peer: %s", stub, this.peer);
			return false;
		}
		try
		{
			stub.cb.invoke(null, this.tusr != null ? this.tusr : this, new StmpPassiveTrans<StmpN2H>(this, stid, begin), begin);
		} catch (IllegalAccessException | IllegalArgumentException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		} catch (InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e.getTargetException()));
			return false;
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
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
		StmpInitiativeTrans<StmpN2H> tt = this.trans.get(tid);
		if (tt == null) /* 找不到事务, 可能已经超时. 直接抛弃, 不将此事件上报应用. */
		{
			if (Log.isDebug())
				Log.debug("can not found Tn2htrans for tid: %08X, may be it was timeout.", tid);
			return true;
		}
		RpcStub stub = Tsc.rpcStubs.get(tt.begin.getClass().getName());
		if (stub == null)
		{
			this.trans.remove(tid);
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, peer: %s", tt.begin.getClass().getName(), this.peer);
			return false;
		}
		Short ret = StmpDec.getShort(root, Stmp.STMP_TAG_RET);
		if (ret == null)
		{
			this.trans.remove(tid); /* 这里有问题(应该在continue完成时移除, 不然无法让事务超时)!!!!!!!!!!!!!!!!!!!!!!!!!!!!. */
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_RET");
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
				this.trans.remove(tid);
				if (Log.isDebug())
					Log.debug("missing required field: STMP_TAG_DAT");
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
		StmpInitiativeTrans<StmpN2H> tt = this.trans.remove(tid);
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
		Tsc.log.trans(tt);
		return true;
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
		//
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
		//
		RpcStub stub = Tsc.rpcStubs.get(msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s, peer: %s", msg, this.peer);
			return false;
		}
		return this.invokeUni(stub, dat);
	}

	/** 单向消息. */
	private final boolean invokeUni(RpcStub stub, byte dat[])
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
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s, e: %s", stub, Log.trace(e));
			return false;
		}
		try
		{
			stub.cb.invoke(null, this.tusr != null ? this.tusr : this, uni);
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
			return false;
		}
		Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);
		if (stid == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_STID");
			return false;
		}
		String sne = StmpDec.getStr(root, Stmp.STMP_TAG_SNE);
		if (sne == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_SNE");
			return false;
		}
		String dne = StmpDec.getStr(root, Stmp.STMP_TAG_DNE);
		if (dne == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_DNE");
			return false;
		}
		if (!sne.equals(this.ne))
		{
			if (Log.isWarn()) /* 出现这种情况的原因在于: 客户端地填写SNE, 即填充自己的SSC句柄是错误. */
				Log.warn("client have a bug? message it not for this H2N, sne: %s, this: %s", sne, this);
			return false;
		}
		if (tag == Stmp.STMP_TAG_TRANS_BEGIN)
			return this.switchBegin(root, stid, sne, dne);
		if (tag == Stmp.STMP_TAG_TRANS_END)
			return this.switchEnd(root, stid, sne, dne);
		if (Log.isWarn())
			Log.warn("unsupported operation, this: %s, sne: %s", this, dne);
		return false;
	}

	/** 事务前转(BEGIN). */
	private final boolean switchBegin(StmpNode root, int stid, String sne, String dne)
	{
		String msg = StmpDec.getStr(root, Stmp.STMP_TAG_MSG);
		if (msg == null)
		{
			if (Log.isWarn())
				Log.warn("missing required field: STMP_TAG_MSG, sne: %s, dne: %s, root: %s", sne, dne, StmpDec.printNode2Str(root));
			return false;
		}
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null) /* 后面还有continue. */
		{
			byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isWarn())
					Log.warn("missing required field: STMP_TAG_DAT, sne: %s, dne: %s, msg: %s, root: %s", sne, dne, msg, StmpDec.printNode2Str(root));
				return false;
			}
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_SWITCH, stid, msg, dat, sne, dne, Stmp.STMP_TAG_TRANS_BEGIN, Short.MIN_VALUE)); /* 缓存. */
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		if (dat == null)
		{
			if (Log.isWarn())
				Log.warn("missing required field: STMP_TAG_DAT, sne: %s, dne: %s, msg: %s, root: %s", sne, dne, msg, StmpDec.printNode2Str(root));
			return false;
		}
		StmpN2H n2h = StmpN2H.nes.get(dne); /* 寻找目标网元. */
		if (n2h == null)
		{
			if (Log.isWarn())
				Log.warn("can not found destination NE, will finish this transaction, sne: %s, dne: %s, msg: %s", sne, dne, msg);
			this.sendSwitchEnd(dne, sne, stid, Ret.RET_FORBIDDEN.getNumber(), (byte[]) null);
			return true;
		}
		n2h.future(v -> n2h.sendSwitchBegin(sne, dne, stid, msg, dat));
		return true;
	}

	/** 事务前转(END). */
	private final boolean switchEnd(StmpNode root, int stid, String sne, String dne)
	{
		Short ret = StmpDec.getShort(root, Stmp.STMP_TAG_RET);
		if (ret == null)
		{
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_RET");
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
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_SWITCH, stid, null, dat, sne, dne, Stmp.STMP_TAG_TRANS_END, ret)); /* 缓存. */
			return true;
		}
		byte dat[] = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
		StmpN2H n2h = StmpN2H.nes.get(dne); /* 寻找目标网元. */
		if (n2h == null)
		{
			if (Log.isDebug())
				Log.debug("can not found destination NE: %s", dne);
			return true;
		}
		n2h.future(v -> n2h.sendSwitchEnd(sne, dne, stid, ret.intValue(), dat));
		return true;
	}

	/** 事务前转. */
	private final boolean invokeSwitch(StmpContinueCache scc)
	{
		StmpN2H n2h = StmpN2H.nes.get(scc.dne); /* 寻找目标网元. */
		if (n2h == null)
		{
			if (Log.isDebug())
				Log.debug("can not found destination NE: %s", scc.dne);
			return true;
		}
		if (scc.tag == Stmp.STMP_TAG_TRANS_BEGIN)
		{
			n2h.future(v -> n2h.sendSwitchBegin(scc.sne, scc.dne, scc.stid, scc.msg, scc.bytes()));
			return true;
		}
		if (scc.tag == Stmp.STMP_TAG_TRANS_END)
		{
			n2h.future(v -> n2h.sendSwitchEnd(scc.sne, scc.dne, scc.stid, scc.ret, scc.bytes()));
			return true;
		}
		return false;
	}

	/** PING. */
	private final boolean stmp_ping(StmpNode root)
	{
		this.send(new byte[] { Stmp.STMP_TAG_TRANS_PONG });
		return true;
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	public void evnDis()
	{
		Iterator<Map.Entry<Integer, StmpInitiativeTrans<StmpN2H>>> iter = this.trans.entrySet().iterator();
		ArrayList<StmpInitiativeTrans<StmpN2H>> tmp = new ArrayList<>();
		while (iter.hasNext())
		{
			tmp.add(iter.next().getValue()); /* 不要在这里调用t.tmCb.accept(t), 原因是在accept中可能有对this.stmpTits的并发操作, 并引起迭代器上的ConcurrentModificationException. */
			iter.remove();
		}
		for (StmpInitiativeTrans<StmpN2H> t : tmp)
		{
			t.tm = true;
			Misc.exeConsumer(t.tmCb, t);
			Tsc.log.trans(t);
		}
		if (this.ne != null)
			StmpN2H.nes.remove(this.ne);
		this.continues.clear();
		if (this.tusr != null) /* 有用户数据. */
		{
			this.tusr.evnDis();
			this.tusr.n2h = null;
			this.tusr = null;
		}
	}

	/** 设置连接上的用户数据. */
	public void setNe(String ne)
	{
		super.setNe(ne);
		StmpN2H.nes.put(ne, this);
	}

	public String toString()
	{
		return Misc.printf2Str("peer: %s, ne: %s", this.peer, this.ne == null ? "NULL" : this.ne);
	}
}
