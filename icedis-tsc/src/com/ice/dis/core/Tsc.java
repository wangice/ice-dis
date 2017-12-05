package com.ice.dis.core;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;

import com.ice.dis.actor.ActorNet;
import com.ice.dis.cfg.Cfg;
import com.ice.dis.cfg.RpcStub;

import misc.Log;
import misc.Misc;
import misc.Net;

public class Tsc
{
	/** 拥有的Tworker数量. */
	public static Tworker[] wk;
	/** 每个线程Tworker对象. */
	public static final ThreadLocal<Tworker> currwk = new ThreadLocal<>();
	/** 服务器句柄. */
	public static ServerSocketChannel srv = null;
	/** Tsc上(N2H)支持的协议, 默认不支持任何协议. */
	public static int protocol = 0x0000;
	/** N2H上的网络消息(基于STMP-PROTOBUF)注册. */
	public static final Map<String, RpcStub> rpcStubs = new HashMap<>();
	/** 系统时间戳, 每Cfg.libtsc_quartz频率更新, (主要用于避免重复调用System.currentTimeMillis()). */
	public static long clock = System.currentTimeMillis();

	public Tsc()
	{

	}

	public static boolean init()
	{
		Cfg.init();
		try
		{
			Tsc.wk = new Tworker[Cfg.libtsc_worker];
			for (int i = 0; i < Tsc.wk.length; i++)
				Tsc.wk[i] = new Tworker(i, Selector.open());
			Misc.sleep(100);
			return true;
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
			return false;
		}
	}

	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于N2H被动接收的事务(鉴权通过前). */
	public static final boolean regBeginEndBeforeAuth(Class<?> begin, Class<?> end, Class<?> handler)
	{
		return Tsc.regMsg(begin, end, null, handler, false, false, false);
	}

	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册. */
	private static final boolean regMsg(Class<?> begin, Class<?> end, Class<?> uni, Class<?> handler, boolean tusr, boolean rpc, boolean service)
	{
		Class<?> msg = uni == null ? begin : uni; /* 为UNI消息时, 没有BEGIN, 使用UNI的className. */
		if (Tsc.rpcStubs.get(msg.getName()) != null)
		{
			Log.error("duplicate msg: %s", msg.getName());
			return false;
		}
		Method m = null;
		if (handler != null)
		{
			String sname = msg.getSimpleName();
			m = Misc.findMethodByName(handler, sname.substring(0, 1).toLowerCase() + sname.substring(1, sname.length()));
			if (m == null)
			{
				Log.error("can not found call back method for msg: %s, handler: %s", msg.getName(), handler.getName());
				return false;
			}
		}
		Log.info("N2H-RPC-STUB, cmd: %s, handler: %s, begin: %s, end: %s, uni: %s, tusr: %s, service: %s", msg.getName(), handler == null ? "NULL" : handler.getName(), //
				begin == null ? "NULL" : begin.getName(), //
				end == null ? "NULL" : end.getName(), //
				uni == null ? "NULL" : uni.getName(), //
				tusr ? "true" : "false", //
				service ? "true" : "false");
		Tsc.rpcStubs.put(msg.getName(), new RpcStub(m, begin, end, uni, tusr, rpc, service, null));
		return true;
	}

	/** 向外提供服务(开启监听端口). */
	public static final boolean publish(int protocol/* 协议支持, 见ActorNet. */)
	{
		Tsc.protocol = protocol;
		if (Tsc.protocol != ActorNet.STMP && Tsc.protocol != ActorNet.MODBUS_TCP)
		{
			Log.fault("unsupported protocol: %04X", protocol);
			return false;
		}
		try
		{
			Tsc.srv = ServerSocketChannel.open();
			Tsc.srv.socket().setReuseAddress(true);
			Tsc.srv.socket().bind(Net.getAddr(Cfg.libtsc_server_addr), 0x800);
			Tsc.srv.configureBlocking(false);
			Log.info("libtsc already listen on %s.", Cfg.libtsc_server_addr);
			Tsc.wk[0].future(v -> Tsc.wk[0].regServerSocketChannel(Tsc.srv));/* only one Tworker thread for connection accept. */
			return true;
		} catch (IOException e)
		{
			if (Log.isError())
				Log.error("LIBTSC_SERVER_ADDR: %s\n%s", Cfg.libtsc_server_addr, Log.trace(e));
			Misc.lazySystemExit();
			return false;
		}
	}

	/** 返回当前工作线程. */
	public static final Tworker getCurrentWorker()
	{
		return Tsc.currwk.get();
	}

	/** 获取Tworker线程. */
	public static int getCurrentTworkerIndex()
	{
		Tworker tworker = Tsc.currwk.get();
		return tworker == null ? -1 : tworker.wk;
	}

	/** 为Actor选择一个工作线程(round-robin). */
	public static final int roundRobinWorkerIndex()
	{
		return (Tworker.rb.incrementAndGet() & 0x7FFFFFFF) % Cfg.libtsc_worker;
	}

	/** 为Actor选择一个工作线程(散列). */
	public static final int hashWorkerIndex(long id)
	{
		return (int) ((id & 0x7FFFFFFFFFFFFFFFL) % (long) Cfg.libtsc_worker);
	}
}
