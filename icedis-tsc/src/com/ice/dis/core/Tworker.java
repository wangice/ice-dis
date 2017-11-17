package com.ice.dis.core;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import com.ice.dis.actor.Actor;

import misc.Log;
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

	/** 线程忙? */
	public volatile boolean busy = false;

	/** 工作线程的个数. */
	public int wk = 1;

	public Tworker(int wk, Selector slt)
	{
		super(ActorType.ITC);
		this.wk = wk;
		this.slt = slt;
	}

	public void run()
	{
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

					} else if (key.isReadable())/* 信道有读数据. */
					{

					} else if (key.isWritable())/* 信道关心写数据. */
					{

					}
					it.remove();
				}
				this.busy = false;
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

}
