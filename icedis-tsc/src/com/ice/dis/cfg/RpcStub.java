package com.ice.dis.cfg;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

import misc.Log;
import misc.Misc;

public class RpcStub
{
	/** 消息标识. */
	public String msg;
	/** 是否有用户数据关联. */
	public boolean tusr;
	/** 是否是一个RPC调用. */
	public boolean rpc;
	/** 是否是一个服务. */
	public boolean service;
	/** 回调地址. */
	public Method cb;
	/** STMP-BEGIN pb消息的newBulider. */
	public Method beginBuilder;
	public String beginName;
	/** STMP-END pb消息的newBulider. */
	public Method endBuilder;
	public String endName;
	/** STMP-UNI pb消息的newBulider. */
	public Method uniBuilder;
	public String uniName;
	/** 如果是一个RPC/SERVICE接口, 表示接口上的注释. */
	public String doc = "none";

	public RpcStub(Method cb, Class<?> begin, Class<?> end, Class<?> uni, boolean tusr, boolean rpc, boolean service, String srvDoc)
	{
		this.tusr = tusr;
		this.rpc = rpc;
		this.service = service;
		this.cb = cb;
		try
		{
			if (begin != null)
			{
				this.beginBuilder = begin.getMethod("newBuilder");
				this.msg = begin.getName();
				this.beginName = this.msg;
			}
			if (end != null)
			{
				this.endBuilder = end.getMethod("newBuilder");
				this.endName = end.getSimpleName();
			}
			if (uni != null)
			{
				this.uniBuilder = uni.getMethod("newBuilder");
				this.msg = uni.getName();
				this.uniName = this.msg;
			}
			if (this.rpc)
				this.doc = Misc.getMethodDoc(cb);
			if (this.service)
				this.doc = srvDoc;
		} catch (Exception e)
		{
			if (Log.isError())
				Log.error("%s", Log.trace(e));
		}
	}

	/** 反射回一个STMP-BEGIN中的pb对象. */
	public final Message newBeginMsg(byte by[]) throws InvalidProtocolBufferException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		return ((AbstractMessage.Builder<?>) this.beginBuilder.invoke(null)).mergeFrom(by).build();
	}

	/** 反射回一个STMP-UNI中的pb对象. */
	public final Message newUniMsg(byte by[]) throws InvalidProtocolBufferException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		return ((AbstractMessage.Builder<?>) this.uniBuilder.invoke(null)).mergeFrom(by).build();
	}

	/** 反射回一个STMP-END中的pb对象. */
	public final Message newEndMsg(byte by[]) throws InvalidProtocolBufferException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		return ((AbstractMessage.Builder<?>) this.endBuilder.invoke(null)).mergeFrom(by).build();
	}

	/** 反射回一个STMP-END中的pb-builder. */
	public final Builder newEndBulider() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		return ((AbstractMessage.Builder<?>) this.endBuilder.invoke(null));
	}

	/** 反射回一个STMP-UNI中的pb-builder. */
	public final Builder newUniBulider() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		return ((AbstractMessage.Builder<?>) this.uniBuilder.invoke(null));
	}

	public String toString()
	{
		return Misc.printf2Str("msg: %s, begin: %s, end: %s, uni: %s", this.msg, this.beginName, this.endName, this.uniName);
	}
}
