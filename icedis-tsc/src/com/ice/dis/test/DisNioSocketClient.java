package com.ice.dis.test;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import misc.Net;

public class DisNioSocketClient
{
	public static final String addr = "127.0.0.1";

	public static final int port = 9981;

	public SocketChannel sc = null;

	public Selector selector = null;

	public void initScoketClient() throws Exception
	{
		sc = SocketChannel.open();
		sc.configureBlocking(false);
		sc.connect(Net.getAddr(DisNioSocketClient.addr, port));

		selector = Selector.open();
		sc.register(selector, SelectionKey.OP_CONNECT);
	}

	public void listen() throws Exception
	{
		System.out.println("客户端启动!");
		while (true)
		{
			selector.select();
			Iterator<SelectionKey> it = selector.selectedKeys().iterator();
			while (it.hasNext())
			{
				SelectionKey key = it.next();
				if (key.isConnectable())/* 连接接续. */
				{
					System.out.println("有连接");
					SocketChannel channel = (SocketChannel) key.channel();
					if (channel.isConnectionPending())/* 准备连接. */
						channel.finishConnect();/* 完成连接. */
					channel.configureBlocking(false);
					// 写入数据.
					channel.write(ByteBuffer.wrap("i like you".getBytes()));
					channel.register(selector, SelectionKey.OP_READ);
					System.out.println("客户端请求链接并发送消息");
				} else if (key.isReadable())/* 信道有可读信息. */
				{
					SocketChannel channel = (SocketChannel) key.channel();
					ByteBuffer buf = ByteBuffer.allocate(1024);
					channel.read(buf);
					buf.flip();
					byte[] by = buf.array();
					System.out.println("message:" + new String(by));
				}
				it.remove();
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		DisNioSocketClient client = new DisNioSocketClient();
		client.initScoketClient();
		client.listen();
	}
}
