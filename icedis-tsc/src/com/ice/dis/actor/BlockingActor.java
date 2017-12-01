package com.ice.dis.actor;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import misc.Log;
import misc.Misc;

public class BlockingActor extends Actor
{
	/** 等待处理的Consumer. */
	public ConcurrentLinkedQueue<Consumer<Void>> cs = new ConcurrentLinkedQueue<>();
	/** size大小. */
	public AtomicInteger size = new AtomicInteger(0);
	/** 拥有的线程. */
	public int tc = 1;
	/** 线程繁忙？ */
	public volatile boolean busy = false;

	public BlockingActor ab = null;

	public BlockingActor(int tc)
	{
		super(ActorType.BLOCKING);
		this.tc = tc;
		this.ab = this;
	}

	/** 添加消费信息. */
	public void push(Consumer<Void> c)
	{
		this.cs.add(c);
		this.size.incrementAndGet();
		synchronized (this)
		{
			this.notify();
		}
	}

	/** 线程忙的状态. */
	public boolean isBusy()
	{
		return this.busy;
	}

	/** 获取Consumer队列大小. */
	public int getSize()
	{
		return this.size.get();
	}

	public void start()
	{
		ExecutorService service = Executors.newFixedThreadPool(tc);/* 线程池. */
		for (int i = 0; i < tc; i++)
		{
			service.submit(() ->
			{
				Log.info("Actor-Blocking worker thread started successfully, name: %s, tid: %d", ab.name, Thread.currentThread().getId());
				while (true)
					ab.run();
			});
		}
	}

	public void run()
	{
		Consumer<Void> c = cs.poll();
		if (c == null)
		{
			synchronized (this)
			{
				try
				{
					this.wait();
				} catch (InterruptedException e)
				{
				}
			}
			c = cs.poll();
		}
		if (c != null)/* 抢占式. */
		{
			this.busy = true;
			this.size.decrementAndGet();
			Misc.exeConsumer(c, null);
			this.busy = false;
		}
	}
}
