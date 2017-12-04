package com.ice.dis.stmp;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.google.protobuf.Message;
import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.core.Tworker;

import misc.Misc;
import stmp.StmpPdu;
import stmp.StmpRpc;

public abstract class StmpNet extends ActorNet {

	/** 事务ID发生器. */
	public int tid = Misc.randInt();

	public StmpNet(ActorType type, SocketChannel sc, ByteBuffer buf) {
		super(type, sc, buf);
		this.protocol = ActorNet.STMP;
	}

	@Override
	public int evnRead(Tworker worker, byte[] by, int _ofst_, int _len_) {
		return 0;
	}

	/** 发送一个STMP-BEGIN消息. */
	public final void sendBegin(int tid, Message begin) {
		byte[] by = begin.toByteArray();
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED - begin.getClass().getName().length();/* 片段长度. */
		if (by.length < seglen) {/* 小于片段长度. */
			this.send(StmpRpc.pkgBegin(begin.getClass().getName(), tid, by));
		}
		int segs = by.length / seglen;/* 片段数. */
		int remain = by.length % seglen;/* 余数. */
		this.send(StmpRpc.pkgBeginWithContinue(begin.getClass().getName(), tid, by, 0, seglen));
		for (int i = 1; i < segs; i++) {
			this.send(StmpRpc.pkgContinue(tid, by, i * seglen, seglen,
					remain != 0 /* 剩余. */ ? true : (i == segs - 1 ? /* 还有. */false : /* 最后一段. */true)));
		}
		if (remain > 0) {
			this.send(StmpRpc.pkgContinue(tid, by, segs * seglen, remain, false));/* 最后一段. */
		}
	}
}
