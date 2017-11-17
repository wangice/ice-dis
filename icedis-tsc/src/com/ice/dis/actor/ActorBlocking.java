package com.ice.dis.actor;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import misc.Log;
import misc.Misc;

public class ActorBlocking extends Actor
{

	public ConcurrentLinkedQueue<Consumer<Void>> cs = new ConcurrentLinkedQueue<>();
	/** cs的个数. */
	public AtomicInteger size = new AtomicInteger();
	/** 线程忙? */
	public volatile boolean busy = false;
	/** 拥有线程的个数. */
	public int tc = 1;

	public ActorBlocking(int tc)
	{
		super(ActorType.BLOCKING);
		this.tc = tc;
		this.start();
	}

	/** 队列的大小. */
	public int size()
	{
		return this.size.get();
	}

	/** 线程忙? */
	public boolean isBusy()
	{
		return this.busy;
	}

	/** 添加进队列中. */
	public void push(Consumer<Void> c)
	{
		this.cs.add(c);
		this.size.incrementAndGet();
		synchronized (this)
		{
			this.notify();
		}
	}

	public void start()
	{
		ActorBlocking ab = this;
		ExecutorService es = Executors.newFixedThreadPool(tc);
		for (int i = 0; i < this.tc; i++)
		{
			es.execute(() ->
			{
				Log.info("Actor-Blocking worker thread started successfully, name: %s, tid: %d", ab.name, Thread.currentThread().getId());
				while (true)
					ab.run();
			});
		}
	}

	public void run()
	{
		Consumer<Void> c = this.cs.poll();
		if (c == null)
		{
			synchronized (this)
			{
				try
				{
					this.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
			c = this.cs.poll();
		}
		if (c != null)
		{
			this.busy = true;
			this.size.decrementAndGet();
			Misc.exeConsumer(c, null);
			this.busy = false;
		}
	}
}
