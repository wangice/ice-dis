package com.ice.dis.actor;

import misc.Net;

/**
 * 
 * N2H连接上的用户数据抽象.
 *
 */
public abstract class Tusr<T1 extends ActorNet, T2> extends Actor
{
	public String id = null;
	public T1 n2h = null;
	public String peer = null;

	public Tusr(String id, T1 n2h)
	{
		super(ActorType.ITC);
		this.wk = n2h.wk; /* 与关联的N2H连接在同个线程. */
		this.id = id;
		this.n2h = n2h;
		this.peer = Net.getRemoteAddr(this.n2h.sc); /* 用于打印. */
	}

	/** 连接是否还在. */
	public final boolean estab()
	{
		return this.n2h != null && this.n2h.est;
	}

	/** 关闭连接. */
	public final void close()
	{
		if (this.n2h != null)
			this.n2h.close();
	}

	/** 静默关闭, 不触发evnDis. */
	public final void closeSlient()
	{
		if (this.n2h != null)
			this.n2h.closeSlient();
	}

	/** 连接上有数据流过(有recv到任何消息). */
	public abstract void evnTraffic(T2 t2);

	/** 与Tusr相关联的N2H连接失去. */
	public abstract void evnDis();
}
