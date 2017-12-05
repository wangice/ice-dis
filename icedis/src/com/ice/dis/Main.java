package com.ice.dis;

import com.ice.dis.cfg.Cfg;
import com.ice.dis.cfg.Tcfg;
import com.ice.dis.core.Tsc;

import misc.Log;
import misc.Misc;

public class Main {

	public static void main(String[] args) {
		Cfg.logenv();
		Log.init("../log/", Cfg.libtsc_log_output);
		//
		Tcfg.init();
		if (!Tsc.init())
			Misc.lazySystemExit();
	}
}
