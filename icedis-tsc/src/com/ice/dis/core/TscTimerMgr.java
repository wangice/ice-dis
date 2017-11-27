package com.ice.dis.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import com.ice.dis.actor.Actor;

import misc.Dateu;
import misc.Log;
import misc.Misc;

public class TscTimerMgr extends Actor
{
	/** 每个Tworker定时器管理器. */
	public static TscTimerMgr[] mgr = null;
	/** 每个时间轮上的刻度数(一个刻度为1秒). */
	private static final int TICKS = 0x400;
	/** 上次刻度跳跃的时间戳. */
	private long lts = Tsc.clock;
	/** 时间轮上的指针位置. */
	private int slot = 0;
	/** Tworker工作线程上的时间轮. */
	public List<Set<TscTimer>> wheel = new ArrayList<>();

	public TscTimerMgr(int wk)
	{
		super(ActorType.ITC);
		this.wk = wk;
	}

	public static final void init()
	{
		TscTimerMgr.mgr = new TscTimerMgr[Tsc.wks.length];
		for (int i = 0; i < Tsc.wks.length; i++)
		{
			TscTimerMgr.mgr[i] = new TscTimerMgr(i);
			for (int c = 0; c < TscTimerMgr.TICKS; c++)
				TscTimerMgr.mgr[i].wheel.add(new HashSet<TscTimer>());
		}
		if (Log.isTrace())
			Log.trace("Tsc timer manager init successfully.");
	}

	/** 跳动一个刻度. */
	public static final void quartz(long now)
	{
		int indx = Tsc.getCurrentWorkIndex();
		if (now - TscTimerMgr.mgr[indx].lts < Dateu.SECOND)/* 不足一秒. */
			return;
		TscTimerMgr.mgr[indx].lts = now;
		TscTimerMgr.mgr[indx].loop();/* 执行定时检查. */
		TscTimerMgr.mgr[indx].slot += 1;/* 指针跳动. */
		TscTimerMgr.mgr[indx].slot = TscTimerMgr.mgr[indx].slot == TscTimerMgr.TICKS ? 0 : TscTimerMgr.mgr[indx].slot;

	}

	/** 添加一个定时器(此函数只允许在工作线程上调用), 并返回定时器存根. 据此存根可在当前线程上取消定时器. */
	public static final TscTimer addTimer(int sec/* sec秒超时. */, Function<Void, Boolean /* 返回true时表示定时器继续生效. */> cb /* 超时后回调. */)
	{
		int indx = Tsc.getCurrentWorkIndex();
		if (indx == -1)
		{
			Log.fault("it`s a bug, stack: %s", Misc.getStackInfo());
			return null;
		}
		return TscTimerMgr.mgr[indx].add(sec, cb);
	}

	/** 添加一个定时器, 一次性使用. */
	public static final TscTimer addTimerOneTime(int sec/* sec秒超时. */, Consumer<Void> cb /* 超时后回调. */)
	{
		return TscTimerMgr.addTimer(sec, tm ->
		{
			Misc.exeConsumer(cb, null);
			return false;
		});
	}

	/** 取消一个定时器. */
	public static final void cancelTimer(TscTimer timer)
	{
		Tsc.wks[timer.wk].future(v -> TscTimerMgr.mgr[timer.wk].cancel(timer));
	}

	/** 执行定时检查. */
	private final void loop()
	{
		Set<TscTimer> timer = this.wheel.get(this.slot);
		List<TscTimer> arr = new ArrayList<>();
		timer.forEach(t -> arr.add(t));/* 为避免并发修改，这里先做一个镜像. */
		//
		List<TscTimer> tmp = new ArrayList<>();
		for (TscTimer tt : arr)
		{
			if (tt.loop == 0)/* 无剩余圈数. */
			{
				Boolean ret = Misc.exeFunction(tt.cb, null);
				if (ret != null && ret.booleanValue())/* 继续等待下次超时. */
					tmp.add(tt);
				timer.remove(tt);
			} else
				tt.loop -= 1;
		}
		//
		for (TscTimer tt : tmp)
		{
			int m = tt.sec % TscTimerMgr.TICKS;
			int pos = this.slot + m;
			tt.slot = pos < TscTimerMgr.TICKS ? pos/* 放置在指针前面(还未到) */ : (pos - TscTimerMgr.TICKS) /* 放置在指针后面(已经过). */;
			this.wheel.get(tt.slot).add(tt);
		}
	}

	/** 添加定时器. */
	private final TscTimer add(int sec/* sec秒超时. */, Function<Void, Boolean /* 返回true时表示定时器继续生效. */> cb /* 超时后回调. */)
	{
		int m = sec % TscTimerMgr.TICKS;
		int pos = this.slot + m;
		TscTimer tt = new TscTimer(sec, (sec / TscTimerMgr.TICKS), pos, cb);
		Set<TscTimer> solt = this.wheel.get(tt.slot);
		solt.add(tt);
		return tt;
	}

	/** 取消定时器. */
	private final boolean cancel(TscTimer timer)
	{
		return this.wheel.get(timer.slot).remove(timer);
	}

	public static class TscTimer
	{
		/** 所属线程. */
		public int wk;
		/** 剩余圈数. */
		public int loop;
		/** sec秒后超时. */
		public int sec;
		/** 在时间轮上的槽. */
		public int slot;

		Function<Void, Boolean> cb;

		public TscTimer(int sec, int loop, int slot, Function<Void, Boolean> cb)
		{
			this.wk = Tsc.getCurrentWorkIndex();
			this.loop = loop;
			this.sec = sec;
			this.slot = slot;
			this.cb = cb;
		}

		public String toString()
		{
			return Misc.printf2Str("wk: %d, sec: %d, loop: %d, slot: %d", this.wk, this.sec, this.loop, this.slot);
		}
	}
}
