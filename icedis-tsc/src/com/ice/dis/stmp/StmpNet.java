package com.ice.dis.stmp;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ice.dis.actor.ActorNet;

public abstract class StmpNet extends ActorNet
{

	public StmpNet(ActorType type, SocketChannel sc, ByteBuffer buf)
	{
		super(type, sc, buf);
		this.protocol = ActorNet.STMP;
	}

}
