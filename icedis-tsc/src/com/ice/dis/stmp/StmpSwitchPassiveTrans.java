package com.ice.dis.stmp;

import com.google.protobuf.Message;

public class StmpSwitchPassiveTrans extends StmpPassiveTrans<StmpH2N>
{
	/** 源网元, 事务发起方. */
	public String sne;
	/** 目标网元. */
	public String dne;

	public StmpSwitchPassiveTrans(StmpH2N h2n, int tid, Message begin, String sne, String dne)
	{
		super(h2n, tid, begin);
		this.sne = sne;
		this.dne = dne;
	}

	/** 事务结束. */
	public final void end(int ret, Message end)
	{
		this.ret = ret;
		this.end = end;
		this.stmpNet.future(v -> {
			this.stmpNet.sendSwitchEnd(this.dne, this.sne, this.tid, this.ret, this.end);
			this.finish();
		});
	}
}