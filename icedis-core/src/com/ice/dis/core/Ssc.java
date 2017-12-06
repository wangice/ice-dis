package com.ice.dis.core;

import java.net.InetSocketAddress;

import com.ice.dis.cfg.Cfg;
import com.ice.dis.cfg.Tcfg;
import com.ice.dis.stmp.StmpH2N;
import com.ice.ssc.srv.SrvComm.NeAuthReq;
import com.ice.ssc.srv.SrvComm.NeAuthRsp;

import misc.Crypto;
import misc.Log;
import misc.Misc;
import stmp.Stmp.Ret;

public class Ssc extends StmpH2N
{

	public Ssc(InetSocketAddress addr) throws Exception
	{
		super(addr);
		if (!this.regRpc())
			throw new Exception("regRpc failed");
		this.connect();
	}

	public void est()
	{
		Log.info("got a connection form SSC: %s", this.addr.toString());
		NeAuthReq.Builder b = NeAuthReq.newBuilder();
		b.setNe("DIS");
		b.setUsr(Tcfg.dis_cfg_ssc_secret.split("@")[0]);
		b.setSalt(Misc.gen0aAkey128());
		b.setSecret(Crypto.sha512StrUpperCase((b.getNe() + b.getSalt() + b.getUsr() + b.getSalt() + Tcfg.dis_cfg_ssc_secret.substring(Tcfg.dis_cfg_ssc_secret.indexOf("@") + 1)).getBytes()));
		this.begin(b.build(), end ->
		{
			if (end.ret != Ret.RET_SUCCESS.getNumber())
			{
				Log.error("auth with SSC failed,SSC: %s", Tcfg.dis_cfg_ssc_addr);
				this.close();
				return;
			}
			NeAuthRsp rsp = (NeAuthRsp) end.end;
			this.setNe(rsp.getNeaddr());
			this.setToken(rsp.getToken());
			Log.info("connect with SSC success: %s", Tcfg.dis_cfg_ssc_addr);
			// 订阅一些网元
		}, tm ->
		{
			if (Log.isWarn())
				Log.warn("auth with SSC timeout, SSC: %s.", Tcfg.dis_cfg_ssc_addr);
		}, Cfg.libtsc_n2h_trans_packet_timeout);
	}

	public void disc()
	{

	}

	public boolean regRpc()
	{
		boolean ret = //
				/*----*/this.regBeginEnd(NeAuthReq.class, NeAuthRsp.class) && //
						0 == "".length();
		return ret;
	}
}
