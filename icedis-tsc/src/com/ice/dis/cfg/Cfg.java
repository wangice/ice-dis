package com.ice.dis.cfg;

public class Cfg
{
	/** 连接上的接收缓冲区尺寸. */
	public static final String LIBTSC_PEER_RCVBUF = "LIBTSC_PEER_RCVBUF";
	/** 连接上的发送缓冲区尺寸. */
	public static final String LIBTSC_PEER_SNDBUF = "LIBTSC_PEER_SNDBUF";
	
	
	public static final int libtsc_tworker = 4;/* tworker工程线程. */
	public static int libtsc_peer_mtu = 0x2000;
	public static int libtsc_peer_rcvbuf = 0x2000;
	public static int libtsc_peer_sndbuf = 0x2000;
}
