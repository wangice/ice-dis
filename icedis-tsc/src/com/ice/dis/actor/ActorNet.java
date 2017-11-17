package com.ice.dis.actor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;

import com.ice.dis.core.Tmempool;
import com.ice.dis.core.Tsc;
import com.ice.dis.core.Tworker;

import misc.Log;
import misc.Net;

public abstract class ActorNet extends Actor
{
	public static final byte STMP_PROTOCOL = 0x01; /* STMP协议. */

	/** 连接上使用的协议. */
	public byte protocol = 0x00;
	/** 连接是否已建立. */
	public boolean est = false;
	/** 连接建立的时间. */
	public long gts = 0L;
	/** 最后收到的消息(指收到的一个完整的包)的时间. */
	public long lts = 0L;
	/** 连接标识. */
	public SocketChannel sc = null;
	/** 连接标识,用于打印. */
	public String peer = null;
	/** 读缓冲区. */
	public ByteBuffer rbb = null;

	/**
	 * 可能的写缓冲区(一些连接上的消息量很小, 将直接依赖TCP发送缓冲区.
	 * 
	 * 但是对于另一些客户端来说, 则可能流量太大, 或有缓存消息带来的临时浪涌, 因此这里设置了一个缓冲区).
	 * 
	 * 缓冲由一些可能大小不等的ByteBuffer组成, 这些ByteBuffer来自一个内存池, 当某个ByteBuffer中的数据全部被发送出去后, 它将归入内存池.
	 * 
	 */
	public ArrayList<ByteBuffer> wbb = null;
	/** 连接上是否注册的写事件. */
	public boolean regWrite = false;
	//
	/** 接收的字节数. */
	public long rxb = 0L;
	/** 接收的消息数. */
	public long rxm = 0L;
	/** 发出的字节数. */
	public long txb = 0L;
	/** 发出的消息数. */
	public long txm = 0L;

	public ActorNet(ActorType type, SocketChannel sc, ByteBuffer rbb)
	{
		super(type);
		this.sc = sc;
		this.peer = Net.getRemoteAddr(sc);
		this.rbb = rbb;
	}

	/** 连接失去事件. */
	public abstract void evnDis();

	/** 网络信息送达. */
	public abstract int evnRead(Tworker worker, byte[] by, int _ofst_, int _len_);

	/** 信息出栈. */
	public void send(byte[] by)
	{

		this.txb = by.length;
		this.txm += 1;
		/** -------------------------------- */
		/**                                  */
		/** 根据TCP发送缓冲区事件进行消息出栈. */
		/**                                  */
		/** -------------------------------- */
		if (wbb != null)
		{

			return;
		}
		try
		{
			if (this.sc.write(ByteBuffer.wrap(by)) != by.length)/* 写入的缓冲区满了. */
			{
				if (Log.isWarn())
					Log.warn("tcp SND_BUF was full we will close this connection, peer: %s", this.peer);
				this.close();
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/** 强制关闭连接(回调evnDis). */
	public void close()
	{
		this.closeSlient();
		this.evnDis();
	}

	public void closeSlient()
	{
		if (!this.est)
			return;
		// TODO: 从当前线程中移除响应信息

	}

	/** 套接字可写. */
	public final boolean evnWrite(SelectionKey key)
	{
		this.regWrite = false;/* 是否需要阻塞. */
		if (this.wbb.size() > 0)
		{
			Iterator<ByteBuffer> it = this.wbb.iterator();
			while (it.hasNext())
			{
				ByteBuffer buf = it.next();
				try
				{
					this.sc.write(buf);
					if (buf.remaining() == 0)/* 已全部写入. */
					{
						Tmempool.free(buf);
						it.remove();
					} else
					{
						this.regWrite = true;
						break;
					}
				} catch (IOException e)
				{
					if (Log.isTrace())
						Log.trace("%s", Log.trace(e));
					this.close();
					return false;
				}
			}
		}
		if (key == null)
			key = this.sc.keyFor(Tsc.getCurrentWorker().slt);
		if (!this.regWrite)
			key.interestOps(SelectionKey.OP_READ);/* 不需要阻塞时,不关心写事件. */
		else
			key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);/* 需要阻塞时，关心写事件. */
		return true;
	}

	/** 进入应用层发送缓冲区, 等待写事件到来时出栈. */
	public final void write2Wbb(byte[] by)
	{
		ByteBuffer bb = null;
		if (this.wbb.size() == 0)/* 无缓存. */
		{
			bb = Tmempool.malloc(by.length);
			bb.put(by);
			bb.flip();
			this.wbb.add(bb);
		} else
		{
			bb = this.wbb.get(this.wbb.size() - 1);/* 有缓存时,取最后一个. */
			if (bb.capacity() - bb.limit() < by.length)/* 剩余空间不足塞入by. */
			{
				bb = Tmempool.malloc(by.length);
				bb.put(by);
				bb.flip();
				this.wbb.add(bb);
			} else
			{
				int pos = bb.position();
				bb.position(bb.limit());
				bb.limit(bb.capacity());
				bb.put(by);
				bb.limit(bb.position());
				bb.position(pos);
			}
		}
		if (!this.regWrite)/* 之前可能无数据发送,所有未注册在写事件上,因此这里尝试的发送. */
			this.evnWrite(null);
	}

	/** 设置应用层的缓冲区, 使之根据selector的写事件来进行消息出栈. */
	public final void enableSndBufAppCache()
	{
		this.wbb = new ArrayList<>();
	}

	/** 归还连接上持有的ByteBuffer块到内存池. */
	public final void relByteBuffs()
	{
		if (this.wbb == null)
			return;
		this.regWrite = false;
		Iterator<ByteBuffer> it = this.wbb.iterator();
		while (it.hasNext())
		{
			Tmempool.free(it.next());
			it.remove();
		}
	}
}
