package com.ice.dis.core;

public class Tsc
{
	/** 工作线程. */
	public static Tworker[] wks;
	/** 当前线程的Tworker对象. */
	public static final ThreadLocal<Tworker> currtwk = new ThreadLocal<>();

	/** 返回当前工作线程. */
	public static final Tworker getCurrentWorker()
	{
		return Tsc.currtwk.get();
	}

	/** 返回当前线程的工作索引. */
	public static int getCurrentWorkIndex()
	{
		Tworker wk = Tsc.currtwk.get();
		return wk == null ? -1 : wk.wk;
	}

}
