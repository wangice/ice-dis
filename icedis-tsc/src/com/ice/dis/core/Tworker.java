package com.ice.dis.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.ice.dis.actor.Actor;
import com.ice.dis.actor.ActorNet;
import com.ice.dis.stmp.StmpN2H;

import misc.Log;
import misc.Misc;
import misc.Net;

public class Tworker extends Actor
{
	/** 多路复用器. */
	public Selector slt;
	/** 管道. */
	public Pipe pipe;
	/** 通知Tworker信号. */
	public ByteBuffer signal = ByteBuffer.allocate(1);
	/** 等待处理Consumer. */
	public ConcurrentLinkedQueue<Consumer<Void>> cs = new ConcurrentLinkedQueue<>();
	/** 线程忙？ */
	public volatile boolean busy = false;
	/** 用于轮询分配工作线程. */
	public static final AtomicInteger rb = new AtomicInteger(0);

	public Tworker(int wk, Selector slt)
	{
		super(ActorType.TIC);
		this.wk = wk;
		this.slt = slt;
		Tworker g = this;
		new Thread(() -> g.run()).start();
	}

	public void run()
	{
		this.pipe = this.initPipe();
		if (this.pipe == null)
			Misc.lazySystemExit();
		try
		{
			while (true)
			{
				this.slt.select();/* 等待接受一个连接. */
				this.busy = true;
				Iterator<SelectionKey> it = slt.selectedKeys().iterator();
				while (it.hasNext())
				{
					SelectionKey key = it.next();
					if (!key.isValid())/* 连接无效. */
					{
						it.remove();
						continue;
					}
					if (key.isAcceptable())/* 连接接入. */
					{

						it.remove();
						continue;
					}
					if (key.isReadable())/* 关心读事件. */
					{

						it.remove();
						continue;
					}
					if (key.isWritable())/* 关心写事件. */
					{
						it.remove();
						continue;
					}
					it.remove();/* 什么都不关心, 则直接移除. */
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
			}
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}
	}

	/** 处理读事件. */
	public void evenAccept(SelectionKey key)
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
				Tworker wk = Tsc.wk[Tsc.roundRobinWorkerIndex()];
				wk.future(x ->
				{
					if (Log.isTrace())
						Log.trace("got a connect : %s", Net.getRemoteAddr(sc));
					ActorNet an = null;
					if (Tsc.protocol == ActorNet.STMP)
						an = new StmpN2H(sc);

				});
			}
		} catch (Exception e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}

	}

	public void push(Consumer<Void> c)/* 有可能有其他线程发送消息处理. */
	{
		try
		{
			this.cs.add(c);
			if (this.busy)
				return;
			synchronized (this)
			{
				this.signal.position(0);
				this.pipe.sink().write(this.signal);
			}
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}
	}

	/** 将网络套接字注册到当前工作线程中. */
	public final boolean regServerSocketChannel(ServerSocketChannel ssc)
	{
		try
		{
			ssc.register(this.slt, SelectionKey.OP_ACCEPT);
			Log.info("the tworker(%d) regist selector", this.wk);
			return true;
		} catch (ClosedChannelException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			Misc.lazySystemExit();
			return false;
		}
	}

	/** 初始化管道. */
	public final Pipe initPipe()
	{
		try
		{
			Pipe pipe = Pipe.open();
			pipe.source().configureBlocking(false);/* 读设置非堵塞. */
			pipe.source().register(slt, SelectionKey.OP_READ);
			pipe.sink().configureBlocking(true);/* 写设置阻塞. */
			Tsc.currwk.set(this);
			return pipe;
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return null;
		}
	}
}
