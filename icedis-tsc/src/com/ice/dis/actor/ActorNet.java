package com.ice.dis.actor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.ice.dis.core.Tmempool;
import com.ice.dis.core.Tsc;
import com.ice.dis.core.Tworker;

import misc.Log;
import misc.Net;

public abstract class ActorNet extends Actor {
	/** STMP协议. */
	public static final byte STMP = 0x01;
	/** MODBUS_TCP协议. */
	public static final byte MODBUS_TCP = 0x02;
	/** 连接上使用的协议. */
	public byte protocol = 0x00;
	/** 连接是否建立. */
	public boolean est = false;
	/** 连接标识, 用于打印. */
	public String peer = null;
	/** 网络连接. */
	public SocketChannel sc = null;
	/** 读缓冲区. */
	public ByteBuffer rbb = null;

	/**
	 * 可能的写缓冲区(一些连接上的消息量很小, 将直接依赖TCP发送缓冲区.
	 * 
	 * 但是对于另一些客户端来说, 则可能流量太大, 或有缓存消息带来的临时浪涌, 因此这里设置了一个缓冲区).
	 * 
	 * 缓冲由一些可能大小不等的ByteBuffer组成, 这些ByteBuffer来自一个内存池, 当某个ByteBuffer中的数据全部被发送出去后,
	 * 它将归入内存池.
	 * 
	 */
	public List<ByteBuffer> wbb = null;
	/** 连接上是否注册的写事件. */
	public boolean regWrite = false;

	public ActorNet(ActorType type, SocketChannel sc, ByteBuffer buf) {
		super(type);
		this.sc = sc;
		this.peer = Net.getRemoteAddr(sc);
		this.rbb = buf;
	}

	/** 连接失去事件. */
	public abstract void evnDis();

	/** 网络信息送达事件. */
	public abstract int evnRead(Tworker worker, byte[] by, int _ofst_, int _len_);

	/** 消息出栈. */
	public void send(byte[] by) {
		if (!this.est) {
			return;
		}
		if (this.wbb != null) {
			this.write2Wbb(by);
			return;
		}
		/** -------------------------------- */
		/**                                  */
		/** 依赖系统管理TCP发送缓冲区. */
		/**                                  */
		/** -------------------------------- */
		try {
			if (this.sc.write(ByteBuffer.wrap(by)) != by.length) {/** 适用于流量较小的连接, 当TCP发送缓冲满时, 认为异常, 断开连接. */
				if (Log.isWarn())
					Log.warn("tcp SND_BUF was full, we will close this connection, peer: %s", this.peer);
				this.close();
				return;
			}
		} catch (IOException e) {
			if (Log.isError()) {
				Log.error("%s", Log.trace(e));
			}
			this.close();
		}
	}

	/** 进入应用层发送缓冲区, 等待写事件到来时出栈. */
	public void write2Wbb(byte[] by) {
		ByteBuffer bb = null;
		if (this.wbb.size() == 0) {/* 无缓存. */
			bb = Tmempool.malloc(by.length);
			bb.put(by);
			bb.flip();
			this.wbb.add(bb);
		} else {
			bb = this.wbb.get(this.wbb.size() - 1);/* 取最后一个缓存. */
			if (bb.capacity() - bb.limit() < by.length) {/* 最后一个不足以塞入by. */
				bb = Tmempool.malloc(by.length);
				bb.put(by);
				bb.flip();
				this.wbb.add(bb);
			} else {
				int pos = bb.position();
				bb.position(bb.limit());
				bb.limit(bb.capacity());
				bb.put(by);
				bb.limit(bb.position());
				bb.position(pos);
			}
		}
		if (!this.regWrite) /* 一些时候, 之前可能无数据发送, 所以未注册在写事件上, 因此这里尝试发送. */
			this.evnWrite(null);
	}

	/** 套接字可写. */
	public final boolean evnWrite(SelectionKey key) {
		this.regWrite = false;/* 是否需要阻塞. */
		if (this.wbb.size() > 0) {
			Iterator<ByteBuffer> it = this.wbb.iterator();
			while (it.hasNext()) {
				ByteBuffer bb = it.next();
				try {
					this.sc.write(bb);
					if (bb.remaining() == 0) {/* 全部写入. */
						Tmempool.free(bb);
						it.remove();
					} else {
						this.regWrite = true;/* 需要阻塞. */
						break;
					}
				} catch (Exception e) {
					if (Log.isError())
						Log.error("%s", Log.trace(e));
					this.close();
					return false;
				}
			}
		}
		if (key == null) {
			key = this.sc.keyFor(Tsc.getCurrentWorker().slt);
		}
		if (!regWrite) {/* 不阻塞, 暂时不关心写事件. */
			key.interestOps(SelectionKey.OP_READ);
		} else {
			key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		}
		return true;
	}

	/** 关闭连接. */
	public void close() {

	}

	/** 静默关闭, 不触发evnDis. */
	public void closeSlient() {
		if (!est) {
			return;
		}
		this.getTworker().removeActorNet(this);
	}

	/** 设置应用层的缓冲区, 使之根据selector的写事件来进行消息出栈. */
	public final void enableSndBufAppCache() {
		this.wbb = new ArrayList<>();
	}

	/** 归还连接上持有的ByteBuffer块到内存池. */
	public final void relByteBuffs() {
		if (this.wbb == null)
			return;
		this.regWrite = false;
		Iterator<ByteBuffer> it = this.wbb.iterator();
		while (it.hasNext()) {
			Tmempool.free(it.next());
			it.remove();
		}
	}
}
