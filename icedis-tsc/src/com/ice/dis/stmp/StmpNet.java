package com.ice.dis.stmp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

import com.ice.dis.actor.ActorNet;
import com.ice.dis.core.Tworker;

import misc.Log;
import misc.Misc;

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

	public int evnRead(Tworker worker, byte[] by, int _ofst_, int _len_)
	{
		return 0;
	}

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
