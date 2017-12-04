package com.ice.dis.stmp;

import com.google.protobuf.Message;

/**
 * 
 * STMP协议上的END返回.
 *
 */
public class TstmpEnd
{
	/** 返回值. */
	public int ret = 0x00;
	/** 数据. */
	public Message end = null;

	public TstmpEnd(int ret, Message end)
	{
		this.ret = ret;
		this.end = end;
	}
}
