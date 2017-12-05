package com.ice.dis.stmp;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import stmp.StmpDec;
import stmp.StmpNode;

public class StmpN2H extends StmpNet
{
	/** 网元唯一标识(鉴权成功后获得). */
	public String ne = null;
	/** 连接上的用户数据. */
	public Tusr<StmpN2H, StmpNode> tusr = null;
	/** 缓存发出的请求, 但未收到的响应. */
	public final Map<Integer, StmpInitiativeTrans<StmpN2H>> trans = new HashMap<>();
	/** 缓存了所有的, 有Tusr的N2H. */
	public static final ConcurrentHashMap<String/* NE. */, StmpN2H> nes = new ConcurrentHashMap<>();

	public StmpN2H(SocketChannel sc, int wk)
	{
		super(ActorType.N2H, sc, ByteBuffer.allocate(Cfg.libtsc_peer_mtu));
		this.est = true;
		this.wk = wk;
		this.gts = Tsc.clock;
	}

	/** 事务开始. */
	public final void begin(Message begin, Consumer<TstmpEnd> endCb/* 事务回调. */, Consumer<StmpInitiativeTrans<StmpN2H>> tmCb/* 超时回调. */, int tm/* 超时时间. */)
	{
		StmpInitiativeTrans<StmpN2H> tt = new StmpInitiativeTrans<StmpN2H>(++this.tid, this, begin, endCb, tmCb, tm);
		this.trans.put(tt.tid, tt);/* 总是缓存. */
		super.sendBegin(tt.tid, tt.begin);
	}

	/** 消息处理. */
	public boolean evnMsg(StmpNode root)
	{
		if (this.tusr != null)
		{
			this.tusr.evnTraffic(root);
		}
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

	public final boolean stmp_begin(StmpNode root)
	{
		Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);/* 获取事务id. */
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
				Log.debug("missing required filed: STMP_TAG_MSG");
			return false;
		}
		RpcStub stub = Tsc.rpcStubs.get(msg);
		if (stub == null)
		{
			if (Log.isDebug())
				Log.debug("unsupported operation, msg: %s,peer: %s", msg, this.peer);
			return false;
		}
		Byte contin = StmpDec.getByte(root, Stmp.STMP_TAG_HAVE_NEXT);
		if (contin != null)/* 后面还有continue. */
		{
			byte[] dat = StmpDec.getBin(root, Stmp.STMP_TAG_DAT);
			if (dat == null)
			{
				if (Log.isDebug())
					Log.debug("missing required filed: STMP_TAG_DAT");
			}
			this.continues.put(stid, new StmpContinueCache(Stmp.STMP_TAG_TRANS_BEGIN, stid, msg, dat, null, null, Byte.MIN_VALUE, Short.MIN_VALUE));
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
	public final boolean invokeBegin(RpcStub stub, byte[] dat, int tid)
	{
		Message begin = null;
		try
		{
			begin = stub.newBeginMsg(dat);
		} catch (InvalidProtocolBufferException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s,e: %s", stub, Log.trace(e));
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
			stub.cb.invoke(null, this.tusr != null ? this.tusr : this, new StmpPassiveTrans<StmpN2H>(this, tid, begin), begin);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			if (Log.isDebug())
				Log.debug("stub: %s,e: %s", stub, Log.trace(e));
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
		return true;
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	public void evnDis()
	{
		Iterator<Entry<Integer, StmpInitiativeTrans<StmpN2H>>> iter = this.trans.entrySet().iterator();
		List<StmpInitiativeTrans<StmpN2H>> tmp = new ArrayList<>();
		while (iter.hasNext())
		{
			tmp.add(iter.next().getValue());/* 不要在这里调用t.tmCb.accept(t), 原因是在accept中可能有对this.stmpTits的并发操作, 并引起迭代器上的ConcurrentModificationException. */
			iter.remove();
		}
		for (StmpInitiativeTrans<StmpN2H> t : tmp)
		{
			t.tm = true;
			Misc.exeConsumer(t.tmCb, t);
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
}
