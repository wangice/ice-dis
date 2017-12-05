package com.ice.ssc.core;

import java.util.concurrent.atomic.AtomicInteger;

import com.ice.dis.core.Tsc;
import com.ice.ssc.srv.SrvComm.NeAuthReq;
import com.ice.ssc.srv.SrvComm.NeAuthRsp;

import misc.Log;

public class Ssc
{
	private final static Ssc inst = new Ssc();

	/** 网元ID生成器. */
	public final AtomicInteger neid = new AtomicInteger(0);

	private Ssc()
	{

	}

	public static final Ssc instance()
	{
		return Ssc.inst;
	}

	public final boolean init()
	{
		return this.regPbStubs();
	}

	/** SSC上的N2H-RPC注册. */
	public final boolean regPbStubs()
	{
		Log.info("\n----------------------------------------------------------------");
		boolean ret = //
				Tsc.regBeginEndBeforeAuth(NeAuthReq.class, NeAuthRsp.class, null) && //
						0 == "".length();
		Log.info("\n----------------------------------------------------------------");
		return ret;
	}
}
