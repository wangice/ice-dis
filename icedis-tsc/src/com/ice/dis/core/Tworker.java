package com.ice.dis.core;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.ice.dis.actor.Actor;
import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.core.TwkStat.TwkStatEnum;
import com.ice.dis.stmp.StmpH2N;
import com.ice.dis.stmp.StmpN2H;
import com.ice.dis.stmp.StmpNet;

import misc.Log;
import misc.Misc;
import misc.Net;

/**
 * 
 * create on: 2017年11月17日 上午9:34:24
 * 
 * @author: ice
 *
 */
public class Tworker extends Actor
{
	/** 等待处理的Consumer. */
	public ConcurrentLinkedQueue<Consumer<Void>> cs = new ConcurrentLinkedQueue<>();
	/** 线程上的多路复用器. */
	public Selector slt = null;
	/** 用于线程间通信. */
	public Pipe pipe = null;
	/** 用于轮询分配工作线程. */
	public static final AtomicInteger rb = new AtomicInteger(0);
	/** 工作线程上所有网络连接(N2H + H2N). */
	public Map<SocketChannel, ActorNet> ans = new HashMap<>();
	/** 线程上所有的H2N, 用于H2N自身检查事务超时和心跳发送. */
	public ArrayList<StmpH2N> h2ns = new ArrayList<>();
	/** 通知Tworker的信号. */
	public final ByteBuffer signal = ByteBuffer.allocate(1);
	/** 线程忙? */
	public volatile boolean busy = false;
	/** 用于线程间通信的管道上的缓冲区. */
	public ByteBuffer bb = ByteBuffer.allocateDirect(64 * 1024); /* 64K, linux-pipe`s size. */
	/** 工程线程上的状态观察. */
	public final TwkStat stat = new TwkStat();

	public Tworker(int wk, Selector slt)
	{
		super(ActorType.ITC);
		this.wk = wk;
		this.slt = slt;
	}

	public void run()
	{
		this.pipe = this.initPipe();
		if (pipe == null)
			Misc.lazySystemExit();
		Log.info("libtsc worker thread started successfully, index: %02X, tid: %d", this.wk, Thread.currentThread().getId());
		while (true)
		{
			try
			{
				slt.select();
				this.busy = true;
				Iterator<SelectionKey> it = slt.selectedKeys().iterator();
				while (it.hasNext())
				{
					SelectionKey key = it.next();
					if (!key.isValid())
					{
						if (Log.isWarn())
						{
							Log.warn("it`s a invalid key, peer: %s", Net.getRemoteAddr((SocketChannel) key.channel()));
							it.remove();
							continue;
						}
					}
					if (key.isAcceptable())/* 有连接到来. */
					{
						this.evnAccept(key);
						it.remove();
						continue;
					}
					if (key.isReadable())/* 信道有读数据. */
					{
						if (!this.evnRead(key))
						{
							it.remove();
							continue;
						}
					}
					if (key.isWritable())/* 信道关心写数据. */
					{
						if (!this.evnWrite(key))
						{
							it.remove();
							continue;
						}
					}
				}
				if (!this.cs.isEmpty())
				{
					Consumer<Void> c = this.cs.poll();
					while (c != null)
					{
						Misc.exeConsumer(c, null);
						c = this.cs.poll();
					}
				}
				this.busy = false;
			} catch (IOException e)
			{
				Log.fault("%s", Log.trace(e));
				Misc.sleep(100);
			}
		}
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/** accept. */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** 连接到达. */
	public final void evnAccept(SelectionKey key)
	{
		try
		{
			ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
			while (true)
			{
				SocketChannel sc = ssc.accept();
				if (sc == null)
					break;
				/**
				 * 
				 * 同C的实现保持一致, 只有一个Tworker处理accept.
				 * 
				 * 但这里无法像C一样拿到套接字的描述字, 亦做不到将某个描述字分配在固定的Tworker上.
				 * 
				 * 因此, 这里使用的方法是对所有的Tworker线程进行轮询.
				 * 
				 */
				Tworker wk = Tsc.wks[Tsc.roundRobinWorkerIndex()]; /* 轮询. */
				wk.future(x ->
				{
					ActorNet an = null;
					if (Tsc.protocol == ActorNet.STMP_PROTOCOL)
						an = new StmpN2H(sc, wk.wk);
					else
						Log.fault("it's a buf forgot set Tsc.Protocal?");
					try
					{
						if (!wk.addActorNet(an))
						{
							Net.closeSocketChannel(sc);
							return;
						}
						wk.setSocketOpt(sc);
						sc.register(wk.slt, SelectionKey.OP_READ);/* 关心读事件. */
						Tsc.log.n2hEstab(an);
						wk.stat.inc(TwkStatEnum.TWK_N2H_TOTAL);
					} catch (Exception e)
					{
						if (Log.isError())
							Log.error("%s", Log.trace(e));
					}
				});
			}
		} catch (Exception e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}
	}

	private final boolean evnRead(SelectionKey key)
	{
		if (key.channel() == this.pipe.source())/* 管道上的消息. */
			return this.evnReadPipe((Pipe.SourceChannel) key.channel());
		ActorNet an = this.ans.get((SocketChannel) key.channel());
		if (an == null)
		{
			key.cancel();
			Log.fault("may be it`s a bug.");
			return false; /* 出现这种情况可能的原因是: 当前key在一轮循环中已经被close, 但在后续的迭代中此key再次出现? */
		}
		return this.evnReadSocket(an, key);/* 网络报文送达. */
	}

	/** 处理管道上的Consumer送达. */
	private final boolean evnReadPipe(Pipe.SourceChannel source)
	{
		try
		{
			int ret = source.read(this.bb);
			while (ret > 0)
			{
				this.bb.position(0);
				ret = source.read(this.bb);
			}
			if (ret != 0)
				Log.fault("it`s a bug.");
			Consumer<Void> c = this.cs.poll();
			while (c != null)
			{
				Misc.exeConsumer(c, null);
				c = this.cs.poll();
			}
		} catch (IOException e)
		{
			if (Log.isWarn())
				Log.warn("%s", Log.trace(e));
		}
		return true;
	}

	/** 处理网络报文送达. */
	private final boolean evnReadSocket(ActorNet an, SelectionKey key)
	{
		SocketChannel sc = (SocketChannel) key.channel();
		boolean flag = true;
		try
		{
			while (flag)
			{
				int ret = sc.read(an.rbb);
				this.stat.incv(TwkStatEnum.TWK_RCV_BYTES, ret == -1 ? 0 : ret);
				an.rxb += (ret == -1 ? 0 : ret);
				if (ret == -1 || this.evnReadMsg(an))/* 连接已断开或消息处理失败. */
				{
					flag = false;
					break;
				}
				if (!an.est)/* 连接可能被重置. */
				{
					flag = false;
					break;
				}
				if (ret == 0)
					break;
			}
		} catch (Exception e)
		{
			flag = false;
			if (Log.isWarn())
				Log.warn("%s", Log.trace(e));
		}
		if (flag)
			return true;
		if (Log.isTrace())
			Log.trace("have a client disconnected: %s", an);
		if (an.est) /* 如果连接还在, 则可能是消息格式本身的问题, 等等. */
		{
			this.removeActorNet(an);
			an.evnDis();
		} else
			; /* 如果连接已经不在, 则一定是已经调用过evnDis和上面的removeActorNet. */
		return false;
	}

	private final boolean evnReadMsg(ActorNet an)
	{
		byte[] by = an.rbb.array();
		int len = an.rbb.position();
		int ofst = 0;
		for (;;)
		{
			if (len < 1)
				break;
			int size = -1;/* -1: 协议异常, 0: 还不是一个完整的报文, >0: 是一个完整的报文, 表示报文的长度. */
			//
			if (an.protocol == ActorNet.STMP_PROTOCOL)
				size = ((StmpNet) an).evnRead(this, by, ofst, len);
			//
			if (size == -1)/* 消息处理异常. */
				return false;
			if (size < 0)
			{
				Log.fault("it`s a bug, size: %d, stack: %s", size, Misc.getStackInfo());
				return false;
			}
			if (size == 0)/* 不是一个完整的包. */
				break;
			ofst += size;
			len -= size;
		}
		try
		{
			if (len != an.rbb.position())
			{
				for (int i = 0; i < len; ++i)
					by[i] = by[i + ofst];
				an.rbb.position(len);
			}
		} catch (Exception e)
		{
			Log.fault("it`s a bug, len: %d, an.rbb.length: %d, exception: %s", len, an.rbb.capacity(), Log.trace(e));
			return false;
		}
		return true;
	}

	/** 套接字可写. */
	private final boolean evnWrite(SelectionKey key)
	{
		ActorNet an = this.ans.get((SocketChannel) key.channel());
		if (an.wbb == null)
		{
			Log.fault("it's a bug");
			key.interestOps(SelectionKey.OP_READ);
			return false;
		}
		return an.evnWrite(key);
	}

	/** 添加一个ActorNet连接. */
	private final boolean addActorNet(ActorNet an)
	{
		ActorNet old = this.ans.get(an.sc);
		if (old != null)
		{
			Log.fault("may be it's a buf,old: %s,new: %s", old, an);
			return false;
		}
		if (an.type == ActorType.H2N)
			this.h2ns.add((StmpH2N) an);
		else
		{
			TscTimerMgr.addTimer(Cfg.libtsc_n2h_zombie, v -> /* 检查僵尸连接. */
			{
				if (an.lts != 0)/* 已经收到过消息. */
					return false;
				if (!an.est)
					return false;
				if (Log.isDebug())
					Log.debug("got a zombie n2h connection: %s, elap: %dmsec, LIBTSC_N2H_ZOMBIE: %dsec", an, Tsc.clock - an.gts, Cfg.libtsc_n2h_zombie);
				this.stat.inc(TwkStatEnum.TWK_N2H_ZOMBIE_TOTAL);
				this.removeActorNet(an);
				return false;
			});
		}
		this.ans.put(an.sc, an);
		return true;
	}

	/** 关闭ActorNet. */
	public final void removeActorNet(ActorNet an)
	{
		an.relByteBuffs();
		this.ans.remove(an.sc);
		an.sc.keyFor(this.slt).cancel();/* 从selector处取消. */
		Net.closeSocketChannel(an.sc);/* 关闭套接字. */
		an.sc = null;
		an.est = false;
		//
		if (an.type == ActorType.N2H)
			this.stat.inc(TwkStatEnum.TWK_N2H_LOST_TOTAL);
	}

	private final Pipe initPipe()
	{
		try
		{
			Pipe pipe = Pipe.open();
			pipe.source().configureBlocking(false);/* 读通道. */
			pipe.source().register(slt, SelectionKey.OP_READ);
			pipe.sink().configureBlocking(false);/* 写通道 阻塞. */
			return pipe;
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return null;
		}

	}

	/** 设置客户端套接字选项. */
	public final void setSocketOpt(SocketChannel sc) throws IOException
	{
		sc.configureBlocking(false);
		sc.setOption(StandardSocketOptions.SO_LINGER, -1);
		sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
		sc.setOption(StandardSocketOptions.SO_RCVBUF, Cfg.libtsc_peer_rcvbuf);
		sc.setOption(StandardSocketOptions.SO_SNDBUF, Cfg.libtsc_peer_sndbuf);
	}
}
