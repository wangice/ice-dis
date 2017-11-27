package com.ice.dis.core;

import java.lang.reflect.Method;
import java.util.HashMap;

import com.ice.dis.cfg.Cfg;
import com.ice.dis.cfg.RpcStub;

import misc.Log;
import misc.Misc;

public class Tsc
{
	/** 工作线程. */
	public static Tworker[] wks;
	/** Tsc上(N2H)支持的协议, 默认不支持任何协议. */
	public static int protocol = 0x0000;
	/** 日志. */
	public static TscLog log = null;
	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册. */
	public static final HashMap<String /* MSG. */, RpcStub> rpcStubs = new HashMap<>();
	/** 当前线程的Tworker对象. */
	public static final ThreadLocal<Tworker> currtwk = new ThreadLocal<>();
	/** 系统时间戳, 每Cfg.libtsc_quartz频率更新, (主要用于避免重复调用System.currentTimeMillis()). */
	public static long clock = System.currentTimeMillis();

	private Tsc()
	{

	}

	public static final void init(TscLog log)
	{
		Tsc.log = log;
		// Cfg.
	}

	/** 返回当前工作线程. */
	public static final Tworker getCurrentWorker()
	{
		return Tsc.currtwk.get();
	}

	/** 返回当前线程的工作索引. */
	public static int getCurrentWorkIndex()
	{
		Tworker wk = Tsc.currtwk.get();
		return wk == null ? -1 : wk.wk;
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

	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于N2H主动发起的事务(鉴权通过前). */
	public static final boolean regBeginEndBeforeAuth(Class<?> begin, Class<?> end)
	{
		return Tsc.regMsg(begin, end, null, null, false, false, false);
	}

	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于N2H主动发起的事务(鉴权通过后). */
	public static final boolean regBeginEnd(Class<?> begin, Class<?> end)
	{
		return Tsc.regMsg(begin, end, null, null, true, false, false);
	}

	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于N2H被动接收的事务(鉴权通过前). */
	public static final boolean regBeginEndBeforeAuth(Class<?> begin, Class<?> end, Class<?> handler)
	{
		return Tsc.regMsg(begin, end, null, handler, false, false, false);
	}

	/** N2H上的网络消息(基于STMP-PROTOBUF的RPC)注册, 用于N2H被动接收的事务(鉴权通过后). */
	public static final boolean regBeginEnd(Class<?> begin, Class<?> end, Class<?> handler)
	{
		return Tsc.regMsg(begin, end, null, handler, true, false, false);
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
