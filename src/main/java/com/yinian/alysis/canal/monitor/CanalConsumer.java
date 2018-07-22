package com.yinian.alysis.canal.monitor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.yinian.alysis.canal.bean.BinlogDeleteInfo;
import com.yinian.alysis.canal.bean.BinlogInsertInfo;
import com.yinian.alysis.canal.bean.BinlogUpdateInfo;
import com.yinian.alysis.canal.bean.HiveUpdateCacheRecd;

public class CanalConsumer implements Runnable {
	private BlockingQueue<BinlogDeleteInfo> delQueue;
	private BlockingQueue<BinlogInsertInfo> insertQueue;
	private BlockingQueue<BinlogUpdateInfo> updateQueue;
	private static final int DEFAULT_RANGE_FOR_SLEEP = 1000;

	public CanalConsumer(BlockingQueue<BinlogDeleteInfo> delQueue, BlockingQueue<BinlogInsertInfo> insertQueue,BlockingQueue<BinlogUpdateInfo> updateQueue) {
		this.delQueue = delQueue;
		this.insertQueue = insertQueue;
		this.updateQueue = updateQueue;
	}

	public void run() {
		System.out.println("启动消费者线程！");
		//标识是删除的消费线程
		new Thread(new Runnable() {
			public void run() {
				while (true) {
					BinlogDeleteInfo data1 = null;
					try {
						data1 = delQueue.poll(1, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (null != data1) {
						System.out.println("正在消费数据：" + data1.toString());
						try {
							Thread.sleep(DEFAULT_RANGE_FOR_SLEEP);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}).start();

		//标识是新增的消费线程
		/*new Thread(new Runnable() {
			public void run() {
				while (true) {
					BinlogInsertInfo data2 = null;
					try {
						data2 = insertQueue.poll(100, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (null != data2) {
						System.out.println("正在消费数据：" + data2.toString());
						try {
							Thread.sleep(DEFAULT_RANGE_FOR_SLEEP);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}).start();*/

		//标识是变更的消费线程
		new Thread(new Runnable() {
			public void run() {
				while (true) {
					BinlogUpdateInfo data3 = null;
					try {
						data3 = updateQueue.poll(1, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (null != data3) {
						System.out.println("正在消费数据：" + data3.toString());
						try {
							Thread.sleep(DEFAULT_RANGE_FOR_SLEEP);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}).start();
	}
	
	/**
	 * 处理更新信息
	 * @param data
	 */
	public void processUpdate(BinlogUpdateInfo data) {
		//HiveUpdateCacheRecd
		HiveUpdateCacheRecd rcd = new HiveUpdateCacheRecd();
		rcd.setSchema(data.getSchemaName());
		rcd.setTableName(data.getTableName());
		StringBuilder sb = new StringBuilder();
		List<Map<String, Object>>  list = data.getUpdateColList();
		for(Map<String, Object> map : list) {
			sb.append(map.get("value")+",");
		}
//		rcd.setFileString(sb.to);
	}
	
	/**
	 * 处理删除信息
	 * @param data
	 */
	public void processUpdate(BinlogDeleteInfo data) {
		
	}
}
