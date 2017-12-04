package com.ice.dis.stmp;

import java.util.Date;
import java.util.function.Consumer;

import com.google.protobuf.Message;
import com.ice.dis.core.Tsc;

import misc.Dateu;
import misc.Misc;

public class StmpInitiativeTrans<T extends StmpNet> {

	/** 事务的发起者. */
	public T stmpNet;
	/** 事务id. */
	public int tid;
	/** 事务上的begin消息. */
	public Message begin;
	/** 事务上的end消息. */
	public TstmpEnd end;
	/** END消息的返回值. */
	public int ret;
	/** 是否超时. */
	public boolean tm;
	/** 超时时间(这是一个未来绝对时间). */
	public long tmts;
	/** 返回值后回调. */
	public Consumer<TstmpEnd> endCb;
	/** 超时回调. */
	public Consumer<StmpInitiativeTrans<T>> tmCb;

	public StmpInitiativeTrans(int tid, T stmpNet, Message begin, Consumer<TstmpEnd> endCb,
			/* END回调. */Consumer<StmpInitiativeTrans<T>> tmCb /* 超时回调. */, int tm /* 超时时间(秒). */) {
		this.stmpNet = stmpNet;
		this.tid = tid;
		this.begin = begin;
		this.endCb = endCb;
		this.tmCb = tmCb;
		this.tmts = Tsc.clock + tm * 1000L;
		this.end = null;
		this.tm = false;
	}

	public String toString() {
		return Misc.printf2Str("msg: %s, tid: %08X, tmts: %s", this.begin.getClass().getName(), this.tid,
				Dateu.parseDateyyyyMMddHHmmssms(new Date(this.tmts)));
	}
}
