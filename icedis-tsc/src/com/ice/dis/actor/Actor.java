package com.ice.dis.actor;

import java.util.function.Consumer;

import com.ice.dis.core.Tsc;
import com.ice.dis.core.Tworker;

import misc.Log;
import misc.Misc;

public abstract class Actor
{
	public enum ActorType
	{
		/** 立刻消费 */
		TIC,
		/** 阻塞. */
		BLOCKING,
		/** 即network to host, 网络到主机的连接. */
		N2H,
		/** 即host to network, 主机到网络的连接. */
		H2N
	}

	/** 线程索引(-1为无效的线程). */
	public int wk = -1;
	/** actor类型. */
	public ActorType type;
	/** actor名. */
	public String name;

	public Actor(ActorType type)
	{
		this.type = type;
		this.name = this.getClass().getSimpleName();
	}

	public void future(Consumer<Void> c)
	{
		if (this.type.ordinal() == ActorType.BLOCKING.ordinal())
		{
			((BlockingActor) this).push(c);
			return;
		}
		if (this.wk == -1)
		{
			Log.fault("it`s a bug, t: %s, %s", this.name, Misc.getStackInfo());
			return;
		}
		if (Tsc.getCurrentTworkerIndex() != this.wk)/* 不是同一线程. */
			Tsc.wks[this.wk].push(c);
		else
			Misc.exeConsumer(c, null);
	}

	/** 得到当前线程. */
	public Tworker getTworker()
	{
		return this.wk == -1 ? null : Tsc.wks[this.wk];
	}
}
