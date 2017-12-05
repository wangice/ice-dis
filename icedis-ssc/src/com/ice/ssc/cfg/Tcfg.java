package com.ice.ssc.cfg;

import misc.Log;
import misc.Misc;

public class Tcfg
{
	private Tcfg()
	{

	}

	public static void init()
	{
		Log.info("\n----------------------------------------------------------------");
		Misc.getEnvs().forEach(o ->
		{
			if (o.getKey().toString().indexOf("SSC_") == 0)
				Log.info("%s=%s", o.getKey(), o.getValue());
		});
		Log.info("\n----------------------------------------------------------------");
		// TODO: 文件监控.
	}

}
