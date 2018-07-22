package com.yinian.alysis.canal.monitor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import com.yinian.alysis.canal.bean.BinlogDeleteInfo;
import com.yinian.alysis.canal.bean.BinlogInsertInfo;
import com.yinian.alysis.canal.bean.BinlogUpdateInfo;

public class TransMonitor {
	public static void main(String[] args) {
		BlockingQueue<BinlogDeleteInfo> delQueue = new LinkedBlockingQueue<>();// 删除队列
		BlockingQueue<BinlogInsertInfo> insertQueue = new LinkedBlockingQueue<>();// 插入队列
		BlockingQueue<BinlogUpdateInfo> updateQueue = new LinkedBlockingQueue<>();// 更新队列
		CanalProducer producer = new CanalProducer(100000, 1200, delQueue, insertQueue, updateQueue);//生产者
		CanalConsumer consumer = new CanalConsumer(delQueue, insertQueue, updateQueue);//消费者
		// 借助Executors
		ExecutorService service = Executors.newCachedThreadPool();
		service.execute(producer);
		service.execute(consumer);
	}
}
