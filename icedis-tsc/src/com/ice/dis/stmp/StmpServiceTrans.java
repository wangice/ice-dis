package com.ice.dis.stmp;

import com.google.protobuf.Message;
import com.ice.dis.core.Tsc;

import stmp.Stmp.Ret;

public class StmpServiceTrans<T extends Message>
{
	public T req;
	public int stid;
	public StmpH2N dne;
	public String sne = null;
	public Message end = null;

	public StmpServiceTrans(String sne, StmpH2N dne, T req, int stid)
	{
		this.sne = sne;
		this.dne = dne;
		this.req = req;
		this.stid = stid;
	}

	/** 事务结束. */
	public final void end(Message end)
	{
		this.end = end;
		this.dne.future(v ->
		{
			this.dne.sendSwitchEnd(this.dne.getNe(), this.sne, this.stid, Ret.RET_SUCCESS.getNumber(), this.end);
			this.finish();
		});
	}

	/** 事务输出. */
	private final void finish()
	{
		Tsc.log.trans(this);
	}
}
