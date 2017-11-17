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
		/** 进程内Actor,附着在某个Tworker上. */
		ITC,
		/** 拥有自己独立线程池或线程池的actor,主要用于IO阻塞操作, 如数据库查询. */
		BLOCKING;
	}

	/** 工作的线程. */
	public int wk = -1;
	/** Actor类型. */
	public ActorType type;
	/** Actor的名称. */
	public String name;

	public Actor(ActorType type)
	{
		this.type = type;
		this.name = this.getClass().getSimpleName();
	}

	public void future(Consumer<Void> c)
	{
		/** 阻塞Actor,则添加到队列中. */
		if (type.ordinal() == ActorType.BLOCKING.ordinal())
		{
			((ActorBlocking) this).push(c);
			return;
		}
		if (wk == -1)
		{
			Log.fault("it is a buf,t : %s, %s", this.name, Misc.getStackInfo());
			return;
		}
		if (Tsc.getCurrentWorkIndex() != this.wk)
		{

		} else
			Misc.exeConsumer(c, null);
	}

	/** 获取当前Twoker. */
	public Tworker getTworker()
	{
		return wk == -1 ? null : Tsc.wks[this.wk];
	}

}
