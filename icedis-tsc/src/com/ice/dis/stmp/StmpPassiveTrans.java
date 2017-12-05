package com.ice.dis.stmp;

import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

import stmp.Stmp.Ret;

/**
 * STMP连接上的被动(passive)事务(接收BEGIN,发送END)抽象.
 * 
 * create on: 2017年12月5日 上午9:44:15
 * 
 * @author: ice
 *
 */
public class StmpPassiveTrans<T extends StmpNet>
{
	/** 事务发起者. */
	public T stmpNet;
	/** 事务id. */
	public int tid;
	/** 事务上BEGIN消息. */
	public Message begin;
	/** 返回结果. */
	public int ret;
	/** 事务上END消息. */
	public Message end;

	public StmpPassiveTrans(T stmpNet, int tid, Message begin)
	{
		this.stmpNet = stmpNet;
		this.tid = tid;
		this.begin = begin;
		this.ret = Ret.RET_SUCCESS.getNumber();
		this.end = null;
	}

	/** 事务结束. */
	public void end(int ret, Message end)
	{
		this.ret = ret;
		this.end = end;
		if (ret == Ret.RET_FAILURE.getNumber()) /* 失败, 关闭连接. */
		{
			this.stmpNet.future(v ->
			{
				this.stmpNet.close();
				this.finish();
			});
			return;
		}
		this.stmpNet.future(v ->
		{
			this.stmpNet.end(this.tid, this.ret, this.end);
			this.finish();
		});
	}

	/** 事务结束. */
	public final void end(ProtocolMessageEnum ret, Message end)
	{
		this.end(ret.getNumber(), end);
	}

	/** 事务结束. */
	public final void end(ProtocolMessageEnum ret)
	{
		this.end(ret.getNumber(), null);
	}

	/** 事务结束. */
	public final void end(Message end)
	{
		this.end(Ret.RET_SUCCESS.getNumber(), end);
	}

	/** 事务输出. */
	protected void finish()
	{
		// Tsc.log.trans(this);
	}
}
