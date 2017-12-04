package com.ice.dis.stmp;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.protobuf.Message;
import com.ice.dis.actor.Tusr;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.core.Tsc;

import misc.Log;
import stmp.Stmp;
import stmp.StmpDec;
import stmp.StmpNode;

public class StmpN2H extends StmpNet {

	/** 连接上的用户数据. */
	public Tusr<StmpN2H, StmpNode> tusr = null;
	/** 缓存发出的请求, 但未收到的响应. */
	public final Map<Integer, StmpInitiativeTrans<StmpN2H>> trans = new HashMap<>();
	/** 缓存了所有的, 有Tusr的N2H. */
	public static final ConcurrentHashMap<String/* NE. */, StmpN2H> nes = new ConcurrentHashMap<>();

	public StmpN2H(SocketChannel sc, int wk) {
		super(ActorType.N2H, sc, ByteBuffer.allocate(Cfg.libtsc_peer_mtu));
		this.est = true;
		this.wk = wk;
		this.gts = Tsc.clock;
	}

	/** 事务开始. */
	public final void begin(Message begin, Consumer<TstmpEnd> endCb/* 事务回调. */,
			Consumer<StmpInitiativeTrans<StmpN2H>> tmCb/* 超时回调. */, int tm/* 超时时间. */) {
		StmpInitiativeTrans<StmpN2H> tt = new StmpInitiativeTrans<StmpN2H>(++this.tid, this, begin, endCb, tmCb, tm);
		this.trans.put(tt.tid, tt);/* 总是缓存. */
		super.sendBegin(tt.tid, tt.begin);
	}

	/** 消息处理. */
	public boolean evnMsg(StmpNode root) {
		if (this.tusr != null) {
			this.tusr.evnTraffic(root);
		}
		switch (root.self.t) {
		case Stmp.STMP_TAG_TRANS_BEGIN:/* 事务开始. */
			return this.stmp_begin(root);
		default:
			return false;
		}
	}

	public final boolean stmp_begin(StmpNode root) {
		Integer stid = StmpDec.getInt(root, Stmp.STMP_TAG_STID);/* 获取事务id. */
		if (stid == null) {
			if (Log.isDebug())
				Log.debug("missing required field: STMP_TAG_STID");
			return false;
		}
		String msg = StmpDec.getStr(root, Stmp.STMP_TAG_MSG);
		if (msg == null) {
			if (Log.isDebug()) {
				Log.debug("missing required filed: STMP_TAG_MSG");
			}
			return false;
		}

		return false;
	}

	public void evnDis() {

	}
}
