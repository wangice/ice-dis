package com.ice.dis.cfg;

import misc.Log;
import misc.Misc;

public class Tcfg {
	/** SSC地址. */
	public static final String DIS_CFG_SSC_ADDR = "DIS_CFG_SSC_ADDR";
	/** 登录到SSC的账号. */
	public static final String DIS_CFG_SSC_SECRET = "DIS_CFG_SSC_SECRET";
	//
	/** http服务器监听地址. */
	public static final String DIS_HTTP_SERVER_ADDRESS = "DIS_HTTP_SERVER_ADDRESS";
	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/**                                                                  */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	public static String dis_cfg_ssc_addr = "192.168.1.2:1224";
	public static String dis_cfg_ssc_secret = "DIS0000@88888888";
	//
	public static String dis_http_server_address = "0.0.0.0:9090";

	private Tcfg() {

	}

	public static void init() {
		Tcfg.dis_cfg_ssc_addr = Misc.getEnvStr(Tcfg.DIS_CFG_SSC_ADDR, Tcfg.dis_cfg_ssc_addr);
		Tcfg.dis_cfg_ssc_secret = Misc.getEnvStr(Tcfg.DIS_CFG_SSC_SECRET, Tcfg.dis_cfg_ssc_secret);
		//
		Tcfg.dis_http_server_address = Misc.getEnvStr(Tcfg.DIS_HTTP_SERVER_ADDRESS, Tcfg.dis_http_server_address);

		//
		Log.info("\n----------------------------------------------------------------");
		Misc.getEnvs().forEach(o -> {
			if (o.getKey().toString().indexOf("DIS_") == 0)
				Log.info("%s=%s", o.getKey(), o.getValue());
		});
		Log.info("\n----------------------------------------------------------------");
	}
}
