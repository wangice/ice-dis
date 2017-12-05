package com.ice.ssc.core;

import com.ice.dis.actor.Tusr;
import com.ice.dis.stmp.StmpN2H;

import misc.Log;
import stmp.StmpNode;

/**
 *
 * @Created on: 2017年1月7日 上午10:05:09
 * @Author: xuzewen@eybond.com
 * 
 */
public class Ne extends Tusr<StmpN2H, StmpNode> implements Comparable<Ne>
{
	public Ne(String id, StmpN2H n2h)
	{
		super(id, n2h);
		n2h.enableSndBufAppCache();
	}

	public void evnTraffic(StmpNode root)
	{

	}

	public void evnDis()
	{
		Router.instance().neStatusChanage(this, false);
		Router.instance().delNe(this.n2h.getNe());
		if (Log.isWarn())
			Log.warn("hava NE connection lost: %s", this.n2h);
	}

	public final int compareTo(Ne o)
	{
		return (this.id + this.hashCode()).compareTo(o.id + o.hashCode());
	}
}
