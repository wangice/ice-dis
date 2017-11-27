package com.ice.dis.stmp;

import java.util.Date;
import java.util.function.Consumer;

import com.google.protobuf.Message;
import com.ice.dis.core.Tsc;

import misc.Dateu;
import misc.Misc;

public class StmpSwitchInitiativeTrans<T extends StmpNet>
{
	/** 源网元. */
	public String sne;
	/** 目标网元. */
	public String dne;
	/** 事务发起者. */
	public T stmpNet;
	/** 事务id. */
	public int tid;
	/** 事务上的BEGIN消息. */
	public Message begin;
	/** 事务上的END消息. */
	public TstmpEnd end;
	/** END消息中的返回值. */
	public int ret;
	/** 是否超时. */
	public boolean tm;
	/** 超时时间(这是一个未来的绝对时间). */
	public long tmts;
	public Consumer<StmpSwitchInitiativeTrans<T>> endCb;
	public Consumer<StmpSwitchInitiativeTrans<T>> tmCb;

	public StmpSwitchInitiativeTrans(String sne, String dne, int tid, T stmpNet, Message begin, Consumer<StmpSwitchInitiativeTrans<T>> endCb, /* END回调. */Consumer<StmpSwitchInitiativeTrans<T>> tmCb /* 超时回调. */, int tm /* 超时时间(秒). */)
	{
		this.sne = sne;
		this.dne = dne;
		this.stmpNet = stmpNet;
		this.tid = tid;
		this.begin = begin;
		this.endCb = endCb;
		this.tmCb = tmCb;
		this.tmts = Tsc.clock + tm * 1000L;
		this.end = null;
		this.tm = false;
	}

	public String toString()
	{
		return Misc.printf2Str("sne: %s, dne: %s, msg: %s, tid: %08X, tmts: %s", this.sne, this.dne, this.begin.getClass().getName(), this.tid, Dateu.parseDateyyyyMMddHHmmssms(new Date(this.tmts)));
	}
}
