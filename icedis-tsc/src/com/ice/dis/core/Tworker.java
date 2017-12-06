package com.ice.dis.core;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.ice.dis.actor.Actor;
import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.stmp.StmpH2N;
import com.ice.dis.stmp.StmpN2H;
import com.ice.dis.stmp.StmpNet;

import misc.Log;
import misc.Misc;
import misc.Net;

public class Tworker extends Actor {
	/** 多路复用器. */
	public Selector slt;
	/** 管道. */
	public Pipe pipe;
	/** 用于线程间通信的管道上的缓冲区. */
	public ByteBuffer bb = ByteBuffer.allocateDirect(64 * 1024); /* 64K, linux-pipe`s size. */
	/** 通知Tworker信号. */
	public ByteBuffer signal = ByteBuffer.allocate(1);
	/** 等待处理Consumer. */
	public ConcurrentLinkedQueue<Consumer<Void>> cs = new ConcurrentLinkedQueue<>();
	/** 线程上所有的H2N, 用于H2N自身检查事务超时和心跳发送. */
	public List<StmpH2N> h2ns = new ArrayList<>();
	/** 所有N2H + H2N. */
	public Map<SocketChannel, ActorNet> ans = new HashMap<>();
	/** 线程忙？ */
	public volatile boolean busy = false;
	/** 用于轮询分配工作线程. */
	public static final AtomicInteger rb = new AtomicInteger(0);

	public Tworker(int wk, Selector slt) {
		super(ActorType.TIC);
		this.wk = wk;
		this.slt = slt;
		Tworker g = this;
		new Thread(() -> g.run()).start();
	}

	public void run() {
		this.pipe = this.initPipe();
		if (this.pipe == null)
			Misc.lazySystemExit();
		Log.info("libtsc worker thread started successfully, index: %02X, tid: %d", this.wk,
				Thread.currentThread().getId());
		while (true) {
			try {
				this.slt.select();/* 等待接受一个连接. */
				this.busy = true;
				Iterator<SelectionKey> it = this.slt.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey key = it.next();
					if (!key.isValid())/* 连接无效. */
					{
						if (Log.isWarn())
							Log.warn("it`s a invalid key, peer: %s", Net.getRemoteAddr((SocketChannel) key.channel()));
						it.remove();
						continue;
					}
					if (key.isAcceptable())/* 连接接入. */
					{
						this.evnAccept(key);
						it.remove();
						continue;
					}
					if (key.isReadable())/* 关心读事件. */
					{
						if (this.evnRead(key)) {
							it.remove();
							continue;
						}
					}
					if (key.isWritable())/* 关心写事件. */
					{
						if (!this.evnWrite(key)) {
							it.remove();
							continue;
						}
					}
					it.remove();/* 什么都不关心, 则直接移除. */
				}
				if (!this.cs.isEmpty()) {
					Consumer<Void> c = this.cs.poll();
					while (c != null) {
						Misc.exeConsumer(c, null);
						c = this.cs.poll();
					}
				}
				this.busy = false;
			} catch (IOException e) {
				if (Log.isError())
					Log.error("%s", Log.trace(e));
			}
		}

	}

	/** 连接接入. */
	public final void evnAccept(SelectionKey key) {
		try {
			ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
			while (true) {
				SocketChannel sc = ssc.accept();
				if (sc == null) {
					Log.info("index:%d, %s",Tsc.getCurrentWorker().wk, "为空");
					break;
				}
				/**
				 * 
				 * 同C的实现保持一致, 只有一个Tworker处理accept.
				 * 
				 * 但这里无法像C一样拿到套接字的描述字, 亦做不到将某个描述字分配在固定的Tworker上.
				 * 
				 * 因此, 这里使用的方法是对所有的Tworker线程进行轮询.
				 * 
				 */
				Tworker wk = Tsc.wks[Tsc.roundRobinWorkerIndex()];
				wk.future(x -> {
					if (Log.isTrace())
						Log.trace("got a connect : %s", Net.getRemoteAddr(sc));
					ActorNet an = null;
					if (Tsc.protocol == ActorNet.STMP)
						an = new StmpN2H(sc, wk.wk);
					try {
						wk.addActorNet(an);
						wk.setSocketOpt(an.sc);
						sc.register(wk.slt, SelectionKey.OP_READ);/* 关心读事件. */
					} catch (Exception e) {
						if (Log.isError())
							Log.error("%s", Log.trace(e));
					}
				});
			}
		} catch (Exception e) {
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}
	}

	/** 处理读事件. */
	public final boolean evnRead(SelectionKey key) {
		if (key.channel() == this.pipe.source())/* 管道上的消息处理. */
			return this.evnReadPipe(this.pipe.source());
		ActorNet an = this.ans.get((SocketChannel) key.channel());
		if (an == null) {
			key.cancel();/* 没有处理. */
			Log.fault("it is a bug");
			return false;
		}
		return this.evnReadSocket(an, key);
	}

	/** 处理写事件. */
	private final boolean evnWrite(SelectionKey key) {
		ActorNet an = this.ans.get((SocketChannel) key.channel());
		if (an.wbb == null) {
			Log.fault("it`s a bug");
			key.interestOps(SelectionKey.OP_READ);
			return false;
		}
		return an.evnWrite(key);
	}

	/** 处理管道上的消息. */
	public final boolean evnReadPipe(Pipe.SourceChannel source) {
		try {
			int ret = source.read(this.bb);
			while (ret > 0) {
				this.bb.position(0);
				ret = source.read(this.bb);
			}
			if (ret != 0) {
				Log.fault("it is a bug");
			}
			Consumer<Void> c = this.cs.poll();
			while (c != null) {
				Misc.exeConsumer(c, null);
				c = this.cs.poll();
			}
		} catch (IOException e) {
			if (Log.isError()) {
				Log.error("%s", Log.trace(e));
			}
		}
		return true;
	}

	/** 处理网络报文送达. */
	public final boolean evnReadSocket(ActorNet an, SelectionKey key) {
		SocketChannel sc = (SocketChannel) key.channel();
		boolean flag = false;
		try {
			while (flag) {
				int ret = sc.read(an.rbb);
				if (ret == -1 || !this.evnReadMsg(an)) {/* 远程连接关闭或消息处理失败. */
					flag = false;
					break;
				}
			}
		} catch (IOException e) {
			if (Log.isError()) {
				Log.error("%s", Log.trace(e));
			}
		}
		return false;
	}

	/** 处理网络报文送达. */
	public final boolean evnReadMsg(ActorNet an) {
		byte[] by = an.rbb.array();
		int len = an.rbb.position();
		int ofst = 0;
		for (;;) {
			if (len < 1) {
				break;
			}
			int size = -1;/* -1: 协议异常, 0: 还不是一个完整的报文, >0: 是一个完整的报文, 表示报文的长度. */
			if (an.protocol == ActorNet.STMP) {
				size = ((StmpNet) an).evnRead(this, by, ofst, len);
			}
			if (size == -1) {/* 消息处理异常. */
				return false;
			}
			if (size < 0) {
				Log.fault("it`s a bug, size: %d, stack: %s", size, Misc.getStackInfo());
				return false;
			}
			if (size == 0) {/* 不是完整的报文. */
				return false;
			}
			ofst += len;
			len -= size;
		}
		try {
			if (len != an.rbb.position()) {
				for (int i = 0; i < len; i++) {
					by[i] = by[i + ofst];
				}
				an.rbb.position(len);
			}
		} catch (Exception e) {
			Log.fault("it`s a bug, len: %d, an.rbb.length: %d, exception: %s", len, an.rbb.capacity(), Log.trace(e));
			return false;
		}
		return true;
	}

	public final void push(Consumer<Void> c)/* 有可能有其他线程发送消息处理. */
	{
		try {
			this.cs.add(c);
			if (this.busy)
				return;
			synchronized (this) {
				this.signal.position(0);
				this.pipe.sink().write(this.signal);
			}
		} catch (IOException e) {
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}
	}

	/** 将网络套接字注册到当前工作线程中. */
	public final boolean regServerSocketChannel(ServerSocketChannel ssc) {
		try {
			ssc.register(this.slt, SelectionKey.OP_ACCEPT);
			Log.info("registered server-socket channel into Tworker[%d] successfully.", this.wk);
			return true;
		} catch (ClosedChannelException e) {
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			Misc.lazySystemExit();
			return false;
		}
	}

	/** 初始化管道. */
	public final Pipe initPipe() {
		try {
			Pipe pipe = Pipe.open();
			pipe.source().configureBlocking(false);/* 读设置非堵塞. */
			pipe.source().register(slt, SelectionKey.OP_READ);
			pipe.sink().configureBlocking(true);/* 写设置阻塞. */
			Tsc.currwk.set(this);
			return pipe;
		} catch (IOException e) {
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return null;
		}
	}

	/** 添加ActorNet. */
	public final void addActorNet(ActorNet an) {
		ActorNet old = this.ans.get(an.sc);
		if (old != null) {
			Log.fault("may be it`s a bug, old: %s, new: %s", old, an);
		}
		// TODO: 检查僵尸连接.
		this.ans.put(an.sc, an);
	}

	/** 关闭ActorNet. */
	public final void removeActorNet(ActorNet an) {
		an.relByteBuffs();
		this.ans.remove(an.sc);
		an.sc.keyFor(this.slt).cancel();/* 从selector处注销. */
		Net.closeSocketChannel(an.sc);/* 关闭套接字. */
		an.sc = null;
		an.est = false;
	}

	public final void setSocketOpt(SocketChannel sc) throws Exception {
		sc.configureBlocking(false);
		sc.setOption(StandardSocketOptions.SO_LINGER, -1);
		sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
		sc.setOption(StandardSocketOptions.SO_RCVBUF, Cfg.libtsc_peer_rcvbuf);
		sc.setOption(StandardSocketOptions.SO_SNDBUF, Cfg.libtsc_peer_sndbuf);
	}
}
