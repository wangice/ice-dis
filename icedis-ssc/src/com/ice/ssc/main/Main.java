package com.ice.ssc.main;

import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.core.Tsc;
import com.ice.ssc.cfg.Tcfg;
import com.ice.ssc.core.Ssc;

import misc.Log;
import misc.Misc;

public class Main
{
	public static void main(String[] args)
	{
		Cfg.logenv();
		Log.init("../log/", Cfg.libtsc_log_output);
		//
		Tcfg.init();
		if (!Tsc.init())
			Misc.lazySystemExit();
		if (!Ssc.instance().init())
			Misc.lazySystemExit();
		Tsc.publish(ActorNet.STMP);

	}
}
