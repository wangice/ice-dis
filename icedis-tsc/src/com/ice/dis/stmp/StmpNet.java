package com.ice.dis.stmp;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ice.dis.actor.ActorNet;
import com.ice.dis.core.Tworker;

public abstract class StmpNet extends ActorNet {

	public StmpNet(ActorType type, SocketChannel sc, ByteBuffer buf) {
		super(type, sc, buf);
		this.protocol = ActorNet.STMP;
	}

	@Override
	public int evnRead(Tworker worker, byte[] by, int _ofst_, int _len_) {
		return 0;
	}

}
