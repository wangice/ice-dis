package com.ice.dis.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.ice.dis.core.TscTimerMgr.TscTimer;

import misc.Misc;

public class TwkStat
{
	public final long items[] = new long[TwkStatEnum.TWK_END.ordinal()];

	public enum TwkStatEnum
	{
		/** 服务器收到的字节数. */
		TWK_RCV_BYTES,
		/** 服务器收到的(有效)消息数. */
		TWK_RCV_MSGS,
		/** 服务器发出的字节数. */
		TWK_SND_BYTES,
		/** 服务器发出的消息数. */
		TWK_SND_MSGS,
		//
		/** 总共生成的N2H数量. */
		TWK_N2H_TOTAL,
		/** 失去的N2H数量. */
		TWK_N2H_LOST_TOTAL,
		/** 获得的僵尸连接数量. */
		TWK_N2H_ZOMBIE_TOTAL,
		//
		/***/
		TWK_END
	};

	/** 自增1. */
	public final void inc(TwkStatEnum item)
	{
		++items[item.ordinal()];
	}

	/** 自增v. */
	public final void incv(TwkStatEnum item, long v)
	{
		items[item.ordinal()] += v;
	}

	/** 获取某项的值. */
	public final long get(TwkStatEnum item)
	{
		return this.items[item.ordinal()];
	}

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** 工作线程上的网络负载. */
	public static final String nload()
	{
		List<long[]> arr = new ArrayList<>();
		for (int i = 0; i < Tsc.wks.length; i++)
			arr.add(Tsc.wks[i].stat.items);
		return Misc.obj2json(arr);
	}

	/** 工作线程上处理的future的个数. */
	public static final String future()
	{
		List<Integer> arr = new ArrayList<>();
		for (int i = 0; i < Tsc.wks.length; i++)
			arr.add(Tsc.wks[i].cs.size());
		return Misc.obj2json(arr);
	}

	/** 工作线程上的网络连接个数, N2H + H2N. */
	public static final String actornets()
	{
		List<Integer> arr = new ArrayList<>();
		for (int i = 0; i < Tsc.wks.length; i++)
			arr.add(Tsc.wks[i].ans.size());
		return Misc.obj2json(arr);
	}

	/** 工作线程上的h2ns个数. */
	public static final String h2ns()
	{
		ArrayList<Integer> arr = new ArrayList<>();
		for (int i = 0; i < Tsc.wks.length; ++i)
			arr.add(Tsc.wks[i].h2ns.size());
		return Misc.obj2json(arr);
	}
	
	/** 工作线程上的定时器个数. */
	public static final String timers()
	{
		ArrayList<Integer> arr = new ArrayList<>();
		for (int i = 0; i < TscTimerMgr.mgr.length; ++i)
		{
			int c = 0;
			for (Set<TscTimer> timers : TscTimerMgr.mgr[i].wheel)
				c += timers.size();
			arr.add(c);
		}
		return Misc.obj2json(arr);
	}
	
	/** 内存池现状. */
	public static final String mempool()
	{
		ArrayList<Integer> arr = new ArrayList<>();
		Tmempool.mempool.forEach(v -> arr.add(v.capacity()));
		return Misc.obj2json(arr);
	}
}
