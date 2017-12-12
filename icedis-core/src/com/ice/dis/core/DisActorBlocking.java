package com.ice.dis.core;

import com.ice.dis.actor.BlockingActor;

public class DisActorBlocking extends BlockingActor
{

	public DisActorBlocking()
	{
		super(4/* 四个线程处理. */);
	}

}
