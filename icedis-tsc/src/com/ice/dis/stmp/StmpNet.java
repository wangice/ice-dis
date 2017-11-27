package com.ice.dis.stmp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

import com.google.protobuf.Message;
import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.core.Tsc;
import com.ice.dis.core.TwkStat.TwkStatEnum;
import com.ice.dis.core.Tworker;

import misc.Log;
import misc.Misc;
import misc.Net;
import stmp.Stmp;
import stmp.StmpDec;
import stmp.StmpNode;
import stmp.StmpPdu;
import stmp.StmpRpc;

public abstract class StmpNet extends ActorNet
{
	/** 网元唯一标识(鉴权成功后获得). */
	public String ne = null;
	/** 事务ID发生器. */
	public int tid = Misc.randInt();

	/** 缓存的, 未接收完全的BEGIN, END, SWITCH, DIALOG中的STMP-CONTINUE. */
	public final HashMap<Integer, StmpContinueCache> continues = new HashMap<>();

	public StmpNet(ActorType type, SocketChannel sc, ByteBuffer rbb)
	{
		super(type, sc, rbb);
		this.protocol = ActorNet.STMP_PROTOCOL;
	}

	/** 发送一个STMP-END消息. */
	public final void end(int tid/* 事务id. */, int ret/* 响应. */, Message end)
	{
		byte[] pd = end == null ? null : end.toByteArray();
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED;/* 片段长度. */
		if (pd == null || pd.length <= seglen) /* 小于一个段长度. */
		{
			this.send(StmpRpc.pkgEnd(ret, tid, pd));
			return;
		}
		int segs = pd.length / seglen;/* 段数. */
		int remain = pd.length % seglen;/* 余数. */
		this.send(StmpRpc.pkgEndWithContinue(tid, ret, pd, 0, seglen));/* 第一段. */
		for (int i = 1; i < segs && this.est; i++)
			this.send(StmpRpc.pkgContinue(tid, pd, i * seglen, seglen, remain != 0/* 有剩余. */ ? true : (i == segs - 1) ? /* 还有. */false : true/* 最后一段. */));
		if (remain > 0 && this.est)
			this.send(StmpRpc.pkgContinue(tid, pd, segs * seglen, remain, false));/* 最后一段. */
	}

	/** 发送一个STMP-UNI消息. */
	public final void uni(Message uni)
	{
		byte[] pb = uni.toByteArray();
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED - uni.getClass().getName().length();
		if (pb.length <= seglen)/* 小于一个段长度. */
			this.send(StmpRpc.pkgUni(uni.getClass().getName(), pb));

		++this.tid;
		int segs = pb.length / seglen;/* 段数. */
		int remain = pb.length % seglen;/* 余数. */
		this.send(StmpRpc.pkgUniWithContinue(uni.getClass().getName(), this.tid, pb, 0, seglen));/* 第一段. */
		for (int i = 1; i < segs && this.est; i++)
			this.send(StmpRpc.pkgContinue(this.tid, pb, i * seglen, seglen, remain != 0/* 有剩余. */ ? true : (i == segs - 1) ? /* 还有. */false : true/* 最后一段. */));
		if (remain > 0 && this.est)
			this.send(StmpRpc.pkgContinue(this.tid, pb, segs * seglen, remain, false)); /* 最后一段. */
	}

	/** 发送一个STMP-BEGIN消息. */
	protected final void sendBegin(int tid, Message begin)
	{
		byte pb[] = begin.toByteArray();
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED - begin.getClass().getName().length(); /* 片段长度. */
		if (pb.length <= seglen) /* 小于一个段长度, 直接begin. */
		{
			this.send(StmpRpc.pkgBegin(begin.getClass().getName(), tid, pb));
			return;
		}
		int segs = pb.length / seglen; /* 段数. */
		int remain = pb.length % seglen; /* 余数. */
		this.send(StmpRpc.pkgBeginWithContinue(begin.getClass().getName(), tid, pb, 0, seglen)); /* 第一段. */
		for (int i = 1; i < segs; ++i)
			this.send(StmpRpc.pkgContinue(tid, pb, i * seglen, seglen, remain != 0 /* 有剩余. */ ? true : (i == segs - 1 ? /* 还有. */false : true /* 最后一段. */)));
		if (remain > 0)
			this.send(StmpRpc.pkgContinue(tid, pb, segs * seglen, remain, false)); /* 最后一段. */
	}

	/** 发送一个STMP-SWITCH(BEGIN)消息. */
	protected final void sendSwitchBegin(String sne, String dne, int stid, Message msg)
	{
		this.sendSwitchBegin(sne, dne, stid, msg.getClass().getName(), msg.toByteArray());
	}

	/** 发送一个STMP-SWITCH(BEGIN)消息. */
	protected final void sendSwitchBegin(String sne, String dne, int stid, String msg, byte[] dat)
	{
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED - sne.length() - dne.length() - msg.length(); /* 片段长度. */
		if (dat.length <= seglen) /* 小于一个段长度. */
		{
			this.send(StmpRpc.pkgSwitchBegin(sne, dne, stid, msg, dat));
			return;
		}
		int segs = dat.length / seglen; /* 段数. */
		int remain = dat.length % seglen; /* 余数. */
		this.send(StmpRpc.pkgSwitchBeginWithContinue(sne, dne, stid, msg, dat, 0, seglen)); /* 第一段. */
		for (int i = 1; i < segs && this.est; ++i)
			this.send(StmpRpc.pkgContinue(stid, dat, i * seglen, seglen, remain != 0 /* 有剩余. */ ? true : (i == segs - 1 ? /* 还有. */false : true /* 最后一段. */)));
		if (remain > 0 && this.est)
			this.send(StmpRpc.pkgContinue(stid, dat, segs * seglen, remain, false)); /* 最后一段. */
	}

	/** 发送一个STMP-SWITCH(END)消息. */
	protected final void sendSwitchEnd(String sne, String dne, int stid, int ret, Message msg)
	{
		byte dat[] = msg == null ? null : msg.toByteArray();
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED - sne.length() - dne.length(); /* 片段长度. */
		if (dat == null || dat.length <= seglen) /* 小于一个段长度. */
		{
			this.send(StmpRpc.pkgSwitchEnd(sne, dne, stid, ret, dat));
			return;
		}
		int segs = dat.length / seglen; /* 段数. */
		int remain = dat.length % seglen; /* 余数. */
		this.send(StmpRpc.pkgSwitchEndWithContinue(sne, dne, stid, ret, dat, 0, seglen)); /* 第一段. */
		for (int i = 1; i < segs && this.est; ++i)
			this.send(StmpRpc.pkgContinue(stid, dat, i * seglen, seglen, remain != 0 /* 有剩余. */ ? true : (i == segs - 1 ? /* 还有. */false : true /* 最后一段. */)));
		if (remain > 0 && this.est)
			this.send(StmpRpc.pkgContinue(stid, dat, segs * seglen, remain, false)); /* 最后一段. */
	}

	/** 发送一个STMP-SWITCH(END)消息. */
	protected final void sendSwitchEnd(String sne, String dne, int stid, int ret, byte[] dat)
	{
		int seglen = Cfg.libtsc_peer_mtu - StmpPdu.STMP_PDU_RESERVED - sne.length() - dne.length(); /* 片段长度. */
		if (dat == null || dat.length <= seglen) /* 小于一个段长度. */
		{
			this.send(StmpRpc.pkgSwitchEnd(sne, dne, stid, ret, dat));
			return;
		}
		int segs = dat.length / seglen; /* 段数. */
		int remain = dat.length % seglen; /* 余数. */
		this.send(StmpRpc.pkgSwitchEndWithContinue(sne, dne, stid, ret, dat, 0, seglen)); /* 第一段. */
		for (int i = 1; i < segs && this.est; ++i)
			this.send(StmpRpc.pkgContinue(stid, dat, i * seglen, seglen, remain != 0 /* 有剩余. */ ? true : (i == segs - 1 ? /* 还有. */false : true /* 最后一段. */)));
		if (remain > 0 && this.est)
			this.send(StmpRpc.pkgContinue(stid, dat, segs * seglen, remain, false)); /* 最后一段. */
	}

	public int evnRead(Tworker worker, byte[] by, int _ofst_, int _len_)
	{
		int ofst = _ofst_;
		int len = _len_;
		while (len > 0)
		{
			int size = 0;
			if (by[ofst] == Stmp.STMP_TAG_TRANS_PING || by[ofst] == Stmp.STMP_TAG_TRANS_PONG)/* 只有一个字节. */
			{
				StmpNode root = new StmpNode();
				root.self.t = by[ofst];
				root.self.l = 0;
				root.self.v = null;
				size = 1;
				if (Log.isRecord())
					Log.record("\n  <-- PEER: %s\n%s", this, StmpDec.print2Str(by, ofst, 1));
				this.lts = Tsc.clock;
				worker.stat.inc(TwkStatEnum.TWK_RCV_MSGS);
				this.rxm += 1;
				Tsc.log.rx(this, by, ofst, len);
				if (this.evnMsg(root))
					return -1;
			} else
			{
				if (len < 3)/* 至少有一个tlv. */
					break;
				int l = (by[ofst + 1] == (byte) 0xFE) ? 3 : ((by[ofst + 1] == (byte) 0xFF) ? 5 : 1);/* 获得len字段的长度. */
				if (len < 1 + l)/* 不够一个tag + len, 如C5 30, 或C5 FE 01 FF. */
					break;
				if (l == 1)
					size = 1/* tag */ + 3/* len字段本身. */ + Net.byte2int(by[ofst + 1])/* val. */;
				else if (l == 3)/* 三个字节表示长度. */
				{
					int s = Net.short2int(Net.byte2short(by, ofst + 2)/* 有两字节表示长度. */);
					size = 1/* tag */ + 3/* len字段本身. */ + s/* len 字段后面的长度. */;
				}
				if (l == 5)
				{
					int s = Net.byte2int(by, ofst + 2) & 0x7FFFFFFF;/* 有四字节表示长度. */
					size = 1/* tag */ + 3/* len字段本身. */ + s/* len 字段后面的长度. */;
					size &= 0x7FFFFFFF;
				}
				if (size > Cfg.libtsc_peer_mtu)
				{
					if (Log.isDebug())
						Log.debug("packet format error(over the LIBTSC_PEER_MTU), we will close this connection, peer: %s, size: %08X", this.peer, size);
					return -1;
				}
				if (size < 1)
				{
					Log.fault("it`s a bug, peer: %s, size: %08X", this.peer, size);
					return -1;
				}
				if (len < size)/* 还未到齐. */
					break;
				worker.stat.inc(TwkStatEnum.TWK_RCV_MSGS);
				this.rxm += 1;
				Tsc.log.rx(this, by, ofst, len);
				StmpNode root = StmpDec.unpack(by, ofst, size);
				if (root == null)
				{
					if (Log.isDebug())
						Log.debug("STMP protocol error, we will close this connection, peer: %s, len: %d, size: %d, %s", this.peer, len, size, Misc.printBytes(by, ofst, size));
					return -1;
				}
				if (Log.isRecord())
					Log.record("\n  <-- PEER: %s\n%s", this.peer, StmpDec.print2Str(by, ofst, size));
				this.lts = Tsc.clock;
				if (this.evnMsg(root))
					return -1;
			}
			ofst += size;
			len -= size;
		}
		return _len_ - len;
	}

	/** 消息事件. */
	public abstract boolean evnMsg(StmpNode root);

	/**
	 * 
	 * 一个网元的连接标识被定义为: NE-ADDR = NE-TYPE + "@" + NE-NAME + "@" + NE-ID;
	 * 
	 * NE-ADDR: 网元连接唯一标识.
	 * 
	 * NE-TYPE: 网元类型, 如CS, DBGW.
	 * 
	 * NE-NAME: 网元名称, 如于标识多个同类型网元的不同进程, 如: CS0000, CS0001.
	 * 
	 * NE-ID: 网元连接临时ID, 连接到SSC时被临时分配.
	 * 
	 */

	/** 返回网元类型和名称, 由NE-TYPE + "@" + NE-NAME + "@"组成. */
	public static final String getNeTypeAndName(String neaddr)
	{
		return neaddr.substring(0, neaddr.lastIndexOf("@") + 1);
	}

	/** 返回网元类型由NE-TYPE + "@"组成. */
	public static final String getNeType(String neaddr)
	{
		return neaddr.substring(0, neaddr.indexOf("@") + 1);
	}

	/** 设置H2N网元唯一标识, 在鉴权通过后设置. */
	public String getNe()
	{
		return ne;
	}

	/** 获得H2N网元唯一标识. */
	public void setNe(String ne)
	{
		this.ne = ne;
	}

	/** 缓存的, 未完成STMP-CONTINUE报文段. */
	public static class StmpContinueCache
	{
		/** STMP消息类型. */
		public byte trans;
		/** 事务ID. */
		public int stid;
		/** 消息内容. */
		public String msg;
		/** 源目标(SWITCH). */
		public String sne;
		/** 目标网元(SWITCH). */
		public String dne;
		/** TAG(SWITCH). */
		public byte tag;
		public short ret;

		/** 片段. */
		private ByteArrayOutputStream bos = new ByteArrayOutputStream();

		public StmpContinueCache(byte trans, int stid, String msg, byte[] seg, String sne, String dne, byte tag, short ret)
		{
			this.trans = trans;
			this.stid = stid;
			this.msg = msg;
			this.sne = sne;
			this.dne = dne;
			this.tag = tag;
			this.ret = ret;
			this.push(seg);
		}

		public final void push(byte[] seg)
		{
			try
			{
				this.bos.write(seg);
			} catch (IOException e)
			{
				Log.fault("it's a bug , exception: %s", Log.trace(e));
			}
		}

		public final byte[] bytes()
		{
			byte[] by = bos.toByteArray();
			return (by == null || by.length < 1) ? null : by;
		}
	}

}
