package com.ice.dis.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class DisNioSocketServer
{

	public static final String addr = "127.0.0.1";

	public static final int port = 9981;

	public ServerSocketChannel ssc = null;

	public Selector sct = null;

	public Pipe pipe = null;

	/** 初始化socketServer. */
	public void initSocketServer(Selector selector)
	{
		try
		{
			ssc = ServerSocketChannel.open();
			ssc.socket().setReuseAddress(true);
			ssc.socket().bind(new InetSocketAddress(port));
			ssc.configureBlocking(false);
			ssc.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public void initSelector()
	{
		try
		{
			sct = Selector.open();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public Pipe initPipe()
	{
		try
		{
			Pipe pipe = Pipe.open();
			pipe.source().configureBlocking(false);/* 设置为非阻塞. */
			pipe.source().register(sct, SelectionKey.OP_READ);
			pipe.sink().configureBlocking(true);/* 设置为阻塞. */
			return pipe;
		} catch (IOException e)
		{
			return null;
		}
	}

	public void run()
	{
		try
		{
			while (true)
			{
				if (this.sct.select(3000) == 0)
					continue;
				Iterator<SelectionKey> it = this.sct.selectedKeys().iterator();
				while (it.hasNext())
				{
					SelectionKey key = it.next();
					if (!key.isValid())
					{
						System.out.println("链接无效了");
						break;
					}
					if (key.isAcceptable())/* 服务器接受就绪. */
					{
						System.out.println("有连接过来.");
						this.handleAccept(key);
					} else if (key.isReadable())/* 客户端感兴趣的读操作. */
					{
						this.handleRead(key);
					} else if (key.isWritable())/* 客户端感兴趣的写操作. */
					{

					}
					it.remove(); /* 移除. */
				}
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private void handleAccept(SelectionKey key)
	{
		try
		{
			SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();
			channel.configureBlocking(false);
			channel.register(this.sct, SelectionKey.OP_READ);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private void handleRead(SelectionKey key)
	{
		try
		{
			SocketChannel channel = (SocketChannel) key.channel();

			ByteBuffer bytebuf = ByteBuffer.allocate(1024 * 6);
			if (!channel.isConnected())/* 管道未链接. */
			{
				System.out.println("链接失效了");
				return;
			}
			int read = channel.read(bytebuf);
			if (read == -1)/* 客户端已经关闭了连接. */
				channel.close();
			else if (read > 0)/* 总读入数据,将信道感兴趣的操作设置为可读可写. */
				key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
			bytebuf.flip();
			byte[] by = bytebuf.array();
			System.out.println("server message: " + new String(by));
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		DisNioSocketServer server = new DisNioSocketServer();
		server.initSelector();
		server.initSocketServer(server.sct);
		server.initPipe();
		server.run();
	}
}
