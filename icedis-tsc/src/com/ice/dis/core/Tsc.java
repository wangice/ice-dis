package com.ice.dis.core;

import java.io.IOException;
import java.nio.channels.Selector;

import com.ice.dis.cfg.Cfg;

import misc.Log;
import misc.Misc;

public class Tsc
{
	/** 拥有的Tworker数量. */
	public static Tworker[] wk;
	/** 每个线程Tworker对象. */
	public static final ThreadLocal<Tworker> currwk = new ThreadLocal<>();
	/** Tsc上(N2H)支持的协议, 默认不支持任何协议. */
	public static int protocol = 0x0000;
	/** 系统时间戳, 每Cfg.libtsc_quartz频率更新, (主要用于避免重复调用System.currentTimeMillis()). */
	public static long clock = System.currentTimeMillis();

	public Tsc()
	{

	}

	public static boolean init()
	{
		try
		{
			Tsc.wk = new Tworker[Cfg.libtsc_tworker];
			for (int i = 0; i < Tsc.wk.length; i++)
				Tsc.wk[i] = new Tworker(i, Selector.open());
			Misc.sleep(100);
			return true;
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return false;
		}
	}

	/** 获取Tworker线程. */
	public static int getCurrentTworkerIndex()
	{
		Tworker tworker = Tsc.currwk.get();
		return tworker.wk == -1 ? -1 : tworker.wk;
	}

	/** 为Actor选择一个工作线程(round-robin). */
	public static final int roundRobinWorkerIndex()
	{
		return (Tworker.rb.incrementAndGet() & 0x7FFFFFFF) % Cfg.libtsc_tworker;
	}
}
