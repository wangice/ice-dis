package com.ice.ssc.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ice.dis.core.Tsc;
import com.ice.dis.stmp.StmpNet;
import com.ice.ssc.srv.SrvComm.NeStatusChangeReq;

import misc.Log;
import misc.Misc;

/**
 *
 * @Created on: 2017年1月7日 下午3:53:50
 * @Author: xuzewen@eybond.com
 * 
 */
public class Router
{
	private static final Router inst = new Router();
	/** 缓存的所有在线的网元. */
	public final ConcurrentHashMap<String /* NEADDR. */, NeStub> nes = new ConcurrentHashMap<>();
	/** 订阅了NE-TYPE状态的所有网元. */
	public final ConcurrentHashMap<String /* NE-TYPE@. */, ConcurrentSkipListSet<Ne> /* 网元. */> subscribes = new ConcurrentHashMap<>();

	private Router()
	{

	}

	public static final Router instance()
	{
		return Router.inst;
	}

	/** 添加一个在线的网元. */
	public final void addNe(String neaddr, Ne ne)
	{
		Router.instance().nes.put(neaddr, new NeStub(ne));
	}

	/** 移除一个掉线的网元. */
	public final void delNe(String neaddr)
	{
		Router.instance().nes.remove(neaddr);
	}

	/** 查询具有某个NE-TYPE的网元. */
	public final ArrayList<String> findNeByType(String netype)
	{
		ArrayList<String> arr = new ArrayList<>();
		Router.instance().nes.forEach((k, v) ->
		{
			if (StmpNet.getNeType(k).equals(netype))
				arr.add(k);
		});
		return arr.size() < 1 ? null : arr;
	}

	/** 订阅网元状态( 锁的用处在于: 当set为null时, 在多线程下会出现同时为null的情况). */
	public final synchronized void subscribeNeStatus(Ne ne, String netype)
	{
		ConcurrentSkipListSet<Ne> set = this.subscribes.get(netype);
		if (set == null)
		{
			set = new ConcurrentSkipListSet<>();
			Router.instance().subscribes.put(netype, set);
		}
		set.add(ne);
		if (Log.isDebug())
			Log.debug("NE: %s subscribe NE-TYPE: %s status successfully: %d", ne.n2h, netype, set.size());
	}

	/** Ne状态发生变化后通知关心它的网元. */
	public final void neStatusChanage(Ne ne, boolean status /* ESTAB | DISCONNECTED */)
	{
		String neaddr = ne.n2h.getNe();
		ConcurrentSkipListSet<Ne> set = Router.instance().subscribes.get(StmpNet.getNeType(neaddr));
		if (set == null || set.size() < 1)
			return;
		Iterator<Ne> it = set.iterator();
		while (it.hasNext())
		{
			Ne n = it.next();
			if (!n.estab()) /* 将已掉线的移除. */
			{
				if (Log.isDebug())
					Log.debug("subscribe-NE(%s) was lost, NE: %s", n.id, neaddr);
				it.remove();
				continue;
			}
			n.future(v ->
			{
				if (!n.estab())
				{
					if (Log.isDebug())
						Log.debug("subscribe-NE(%s) was lost, NE: %s", n.id, neaddr);
					return;
				}
				NeStatusChangeReq.Builder b = NeStatusChangeReq.newBuilder();
				b.setNe(neaddr);
				b.setStatus(status);
				if (Log.isDebug())
					Log.debug("have a NE(%s) status changed: %s, we will notify: %s", status ? "ESTAB" : "DISCONNECTED", neaddr, n.n2h.getNe());
				n.n2h.begin(b.build(), endCb -> Misc.donothing(), tmCb -> Log.warn("notify NE(%s) status(%s) change timeout, dne: %s", neaddr, status ? "ESTAB" : "DISCONNECTED", n.n2h.getNe()), 10);
			});
		}
	}

	public static class NeStub
	{
		/** 网元. */
		public Ne ne = null;
		/** 连接到SSC的时间戳. */
		public long ts = Tsc.clock;

		public NeStub(Ne ne)
		{
			this.ne = ne;
		}
	}
}
