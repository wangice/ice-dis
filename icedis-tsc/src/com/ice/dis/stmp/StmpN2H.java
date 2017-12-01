package com.ice.dis.stmp;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ice.dis.cfg.Cfg;

public class StmpN2H extends StmpNet
{

	public StmpN2H(SocketChannel sc)
	{
		super(ActorType.N2H, sc, ByteBuffer.allocate(Cfg.libtsc_peer_mtu));
	}

}
