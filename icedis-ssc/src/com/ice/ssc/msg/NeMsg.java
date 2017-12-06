package com.ice.ssc.msg;

import com.ice.dis.stmp.StmpN2H;
import com.ice.dis.stmp.StmpPassiveTrans;
import com.ice.ssc.core.Ne;
import com.ice.ssc.core.Router;
import com.ice.ssc.core.Ssc;
import com.ice.ssc.srv.SrvComm.NeAuthReq;
import com.ice.ssc.srv.SrvComm.NeAuthRsp;

import misc.Log;
import misc.Misc;

public class NeMsg
{
	/** 鉴权. */
	public static final void neAuthReq(StmpN2H n2h, StmpPassiveTrans<StmpN2H> trans, NeAuthReq req)
	{
		String token = Misc.gen0aAkey256();
		String neaddr = Misc.printf2Str("%s@%s@%08X", req.getNe(), req.getUsr(), Ssc.instance().neid.incrementAndGet());
		Ne ne = new Ne(neaddr, n2h);
		n2h.tusr = ne;
		n2h.setNe(ne.id);
		Log.info("have a NE auth successfully, N2H: %s", n2h);
		if (Log.isInfo())
			Log.info("have a NE auth successfully, N2H: %s", n2h);
		Router.instance().addNe(n2h.getNe(), ne);
		Router.instance().neStatusChanage(ne, true);
		//
		trans.end(NeAuthRsp.newBuilder().setNeaddr(neaddr).setToken(token).build());
	}
}
