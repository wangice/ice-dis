package com.ice.dis.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;

import misc.Log;
import misc.Misc;
import misc.Net;

public class HttpReq
{
	public HttpExchange exc;
	public Map<String, String> pars = new HashMap<>();
	public boolean jsonp;
	public String callback;

	public HttpReq(HttpExchange exc)
	{
		this.exc = exc;
		this.parsePars();
		this.jsonp = !Misc.isNull(this.getParStr("callback"));
		callback = this.getParStr("callback");
	}

	/** 解析传入参数. */
	public final void parsePars()
	{
		try
		{
			String[] pars = this.exc.getRequestURI().getRawQuery().split("&");
			if (pars == null || pars.length < 1)
				return;
			for (int i = 0; i < pars.length; i++)
			{
				String[] kv = pars[i].split("=");
				if (kv == null || kv.length < 1)
					continue;
				this.pars.put(kv[0], kv.length == 1 ? "" : Net.urlDecode(kv[1]));
			}
		} catch (Exception e)
		{
			return;
		}
	}

	public final String getParStr(String par)
	{
		return Misc.trim(this.pars.get(par));
	}

	/** 获取到action及后面的参数(注意, 这是未经uri.decode处理的). */
	public final String getAction()
	{
		try
		{
			String query = this.exc.getRequestURI().toString();
			String action = query.substring(query.indexOf("&action"), query.length());
			return !jsonp ? action : action.substring(0, action.indexOf("&callback"));
		} catch (Exception e)
		{
			return null;
		}
	}

	/** 获取远端访问地址. */
	public final String getIp()
	{
		try
		{
			List<String> arr = this.exc.getRequestHeaders().get("X-real-ip");
			if (arr != null && !arr.isEmpty())
				return arr.get(0);
			arr = this.exc.getRequestHeaders().get("X-forwarded-for");
			if (arr != null && !arr.isEmpty())
				return arr.get(0);
			return "unknown";
		} catch (Exception e)
		{
			if (Log.isDebug())
				Log.debug("%s", Log.trace(e));
			return "unknown";
		}
	}
}
