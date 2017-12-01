package com.ice.dis.actor;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class ActorNet extends Actor
{
	/** STMP协议. */
	public static final byte STMP = 0x01;
	/** MODBUS_TCP协议. */
	public static final byte MODBUS_TCP = 0x02;
	/** 连接上使用的协议. */
	public byte protocol = 0x00;
	/** 网络连接. */
	public SocketChannel sc = null;
	/** 读缓冲区. */
	public ByteBuffer rbb = null;

	public ActorNet(ActorType type, SocketChannel sc, ByteBuffer buf)
	{
		super(type);
		this.sc = sc;
		this.rbb = buf;
	}

}
