package com.ice.dis.core;

import com.google.protobuf.Message;
import com.ice.dis.actor.ActorNet;

/**
 * 
 * 事务日志接口, 所有已完成的事务都是通过此接口进行输出.
 * 
 * create on: 2017年11月22日 下午4:23:50
 * 
 * @author: ice
 *
 */
public interface TscLog
{
	/** 定时器振荡, 需要注意是, 此函数会在每个Tworker线程上都触发一次. 因此, 在quartz函数中操作ThreadLocal变量是安全的. */
	public void quartz(long now);

	/** 定时器震荡, 仅在主线程. */
	public void quartzMain(long now);

	/** 收到一个N2H连接. 需要注意的是, 此函数会在N2H所在的线程调用. 因此, 直接操作N2H是安全的. */
	public void n2hEstab(ActorNet n2h);

	/** 事务. */
	public void trans(Object tt);

	/** 发出STMP-UNI消息. */
	public void sentUni(ActorNet an, Message uni);

	/** 收到STMP-UNI消息. */
	public void receivedUni(ActorNet an, Message uni);

	/** ---------------------------------------------------------------- */
	/**                                                                  */
	/** RX/TX. */
	/**                                                                  */
	/** ---------------------------------------------------------------- */
	/** ActorNet上的入栈消息. */
	public void rx(ActorNet an, byte by[], int ofst, int len);

	/** ActorNet上的出栈消息. */
	public void tx(ActorNet an, byte by[], int ofst, int len);
}
