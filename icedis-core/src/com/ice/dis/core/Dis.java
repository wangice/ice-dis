package com.ice.dis.core;

import com.ice.dis.cfg.Tcfg;

import misc.Log;
import misc.Net;

public class Dis
{

	private static final Dis inst = new Dis();
	/** 与SSC之间连接句柄. */
	public static Ssc ssc = null;

	public Dis()
	{

	}

	public static final Dis instance()
	{
		return Dis.inst;
	}

	/** DIS进程初始化. */
	public final boolean init()
	{
		if (!this.setupH2ns())
			return false;
		return true;
	}

	/** 开启所有H2N连接. */
	public final boolean setupH2ns()
	{
		try
		{
			Dis.ssc = new Ssc(Net.getAddr(Tcfg.dis_cfg_ssc_addr));
			return true;
		} catch (Exception e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return false;
		}
	}

}
