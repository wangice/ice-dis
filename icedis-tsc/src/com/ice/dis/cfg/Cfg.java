package com.ice.dis.cfg;

import misc.Log;
import misc.Misc;

public class Cfg
{
	/** 服务地址. */
	public static final String LIBTSC_SERVER_ADDR = "LIBTSC_SERVER_ADDR";
	/** 消息总线线程数. */
	public static final String LIBTSC_WORKER = "LIBTSC_WORKER";
	/** 连接上的接收缓冲区尺寸. */
	public static final String LIBTSC_PEER_RCVBUF = "LIBTSC_PEER_RCVBUF";
	/** 连接上的发送缓冲区尺寸. */
	public static final String LIBTSC_PEER_SNDBUF = "LIBTSC_PEER_SNDBUF";
	/** H2N重连等待(秒). */
	public static final String LIBTSC_H2N_RECONN = "LIBTSC_H2N_RECONN";
	/** 定时器精度(毫秒). */
	public static final String LIBTSC_QUARTZ = "LIBTSC_QUARTZ";

	/** 日志级别. */
	public static final String LIBTSC_LOG_LEVEL = "LIBTSC_LOG_LEVEL";
	/** 日志吐出方式. */
	public static final String LIBTSC_LOG_OUTPUT = "LIBTSC_LOG_OUTPUT";
	/** 是否只吐出指定级别的日志. */
	public static final String LIBTSC_LOG_SINGLE = "LIBTSC_LOG_SINGLE";
	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/** 默认值. */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	public static String libtsc_server_addr = "127.0.0.1:1224";
	public static int libtsc_worker = 4;/* tworker工程线程. */
	public static int libtsc_peer_mtu = 0x2000;
	public static int libtsc_peer_rcvbuf = 0x2000;
	public static int libtsc_peer_sndbuf = 0x2000;
	public static long libtsc_h2n_reconn = 3;
	public static int libtsc_n2h_trans_packet_timeout = 15;
	public static long libtsc_quartz = 1000;
	//
	public static String libtsc_log_level = "DEBUG";
	public static int libtsc_log_output = Log.OUTPUT_STDOUT;
	public static boolean libtsc_log_single = false;

	public Cfg()
	{

	}

	public static final void init()
	{
		Cfg.libtsc_server_addr = Misc.getSetEnvStr(Cfg.LIBTSC_SERVER_ADDR, Cfg.libtsc_server_addr);
		Cfg.libtsc_worker = Misc.getSetEnvInt(Cfg.LIBTSC_WORKER, Cfg.libtsc_worker);
		Cfg.libtsc_h2n_reconn = Misc.getSetEnvInt(Cfg.LIBTSC_H2N_RECONN, (int) Cfg.libtsc_h2n_reconn);
		Cfg.libtsc_quartz = Misc.getSetEnvInt(Cfg.LIBTSC_QUARTZ, (int) Cfg.libtsc_quartz);
		//
		Cfg.logenv();
		Log.info("\n----------------------------------------------------------------");
		Misc.getEnvs().forEach(o ->
		{
			if (o.getKey().toString().indexOf("LIBTSC_") == 0)
				Log.info("%s=%s", o.getKey(), o.getValue());
		});
		Log.info("\n----------------------------------------------------------------");
	}

	/** 日志环境初始化. */
	public static final void logenv()
	{
		Cfg.libtsc_log_level = Misc.getSetEnvStr(Cfg.LIBTSC_LOG_LEVEL, Cfg.libtsc_log_level);
		String str = Misc.getSetEnvStr(Cfg.LIBTSC_LOG_OUTPUT, "STDOUT");
		Cfg.libtsc_log_output = str.indexOf("STDOUT") >= 0 ? Log.OUTPUT_STDOUT : 0;
		Cfg.libtsc_log_output = str.indexOf("FILE") >= 0 ? Cfg.libtsc_log_output | Log.OUTPUT_FILE : Cfg.libtsc_log_output;
		Cfg.libtsc_log_single = !"false".equals(Misc.getSetEnvStr(Cfg.LIBTSC_LOG_SINGLE, "false"));
		Log.setLevel(Cfg.libtsc_log_level);
		Log.setOutput(Cfg.libtsc_log_output);
		Log.single(Cfg.libtsc_log_single);
	}
}
