package com.ice.dis.core;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 为发送缓冲区准备的ByteBuffer内存池, 这是一个全局共享内存池.
 * 
 */
public class Tmempool {
	/** 内存池. */
	public static final ConcurrentLinkedQueue<ByteBuffer> mempool = new ConcurrentLinkedQueue<ByteBuffer>();

	/** 从池中返回/分配一个ByteBuffer. */
	public static final ByteBuffer malloc(int size) {
		ByteBuffer bb = Tmempool.mempool.poll();
		if (bb == null) /* 缓冲池中没有ByteBuffer时, 新建一个. */
		{
			bb = ByteBuffer.allocate(size * 4);
			return bb;
		}
		if (bb.capacity() < size) /* 块空间不足塞入by时, 丢掉老的, 并创建一个新的. */
			bb = ByteBuffer.allocate(size * 4);
		return bb;
	}

	/** 归还ByteBuffer到内存池. */
	public static final void free(ByteBuffer bb) {
		bb.position(0);
		bb.limit(bb.capacity());
		Tmempool.mempool.add(bb);
	}

	/** 清空内存池. */
	public static final void clear() {
		Tmempool.mempool.clear();
	}
}
