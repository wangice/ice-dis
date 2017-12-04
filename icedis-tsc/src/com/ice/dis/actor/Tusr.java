package com.ice.dis.actor;

import misc.Net;

/**
 * N2H连接上的用户数据抽象
 * 
 * 创建时间： 2017年12月5日 上午12:15:33
 * 
 * @author ice
 *
 */
public abstract class Tusr<T1 extends ActorNet, T2> extends Actor {

	public String id;
	public T1 n2h = null;
	public String peer;

	public Tusr(String id, T1 n2h) {
		super(ActorType.TIC);
		this.wk = n2h.wk;/** 与n2h同一个线程. */
		this.id = id;
		this.n2h = n2h;
		this.peer = Net.getRemoteAddr(this.n2h.sc); /* 用于打印. */
	}

	/** 连接是否还在. */
	public final boolean estab() {
		return this.n2h != null && this.n2h.est;
	}

	/** 关闭连接. */
	public final void close() {
		if (this.n2h != null) {
			this.n2h.close();
		}
	}

	/** 静默关闭. */
	public final void closeSlient() {
		if (this.n2h != null) {
			this.n2h.closeSlient();
		}
	}

	/** 连接上有数据流过(有recv到任何消息). */
	public abstract void evnTraffic(T2 t2);

	/** 失去连接事件. */
	public abstract void evnDis();
}
