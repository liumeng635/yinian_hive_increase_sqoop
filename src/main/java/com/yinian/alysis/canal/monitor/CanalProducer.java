package com.yinian.alysis.canal.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.yinian.alysis.canal.bean.BinlogDeleteInfo;
import com.yinian.alysis.canal.bean.BinlogInsertInfo;
import com.yinian.alysis.canal.bean.BinlogUpdateInfo;
import com.yinian.alysis.canal.client.CanalClient;
import com.yinian.alysis.canal.client.MysqlJdbcComm;

public class CanalProducer implements Runnable{
	 private static Logger log = LoggerFactory.getLogger(CanalProducer.class);
	 private int batchSize;
	 @SuppressWarnings("unused")
	private int totalEmtryCount;
	 BlockingQueue<BinlogDeleteInfo> delQueue;//删除队列
	 BlockingQueue<BinlogInsertInfo> insertQueue;//插入队列
	 BlockingQueue<BinlogUpdateInfo> updateQueue;//更新队列
	 
	 public CanalProducer(int batchSize,int totalEmtryCount,BlockingQueue<BinlogDeleteInfo> delQueue,BlockingQueue<BinlogInsertInfo> insertQueue,BlockingQueue<BinlogUpdateInfo> updateQueue) {
		 this.batchSize=batchSize;
		 this.totalEmtryCount=totalEmtryCount;
		 this.delQueue = delQueue;
		 this.insertQueue = insertQueue;
		 this.updateQueue = updateQueue;
	 }
	 
	 @Override
	 public void run() {
		 CanalClient client = CanalClient.getInstance();
    	 // 创建链接  
        CanalConnector connector = client.getConnector();  
        int emptyCount = 0;  
        try {  
            connector.connect();  
            connector.subscribe(".*\\..*");  
            connector.rollback();  
            while (true) {//emptyCount < totalEmtryCount
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据  
                long batchId = message.getId();
                int size = message.getEntries().size();
                try {
					if (batchId == -1 || size == 0) {  
					    emptyCount++;  
					    log.info("empty count : " + emptyCount);  
					    try {  
					        Thread.sleep(1);  
					    } catch (InterruptedException e) {  
					        e.printStackTrace();  
					    }  
					} else {  
					    emptyCount = 0;  
					    printEntry(message.getEntries());//处理mysql记录变更信息到hive
					}
					connector.ack(batchId); // 提交确认  
				} catch (Exception e) {
					connector.rollback(batchId); // 处理失败, 回滚数据  
					e.printStackTrace();
				}
            }  
//            log.info("empty too many times");  
        } finally {  
            connector.disconnect();  
        } 
	 }
	 
	 public void printEntry(@NotNull List<Entry> entrys) {
	    	String schemaName = "";
	    	String tableName = "";
	    	EventType eventType = null;
	        for (Entry entry : entrys) {  
	            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {  
	                continue;  
	            }  
	            RowChange rowChage = null;  
	            try {  
	                rowChage = RowChange.parseFrom(entry.getStoreValue());
	            } catch (Exception e) {  
	                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),e);  
	            }  
	  
	            eventType = rowChage.getEventType();//操作类型
	            schemaName = entry.getHeader().getSchemaName();//数据库schema
	            tableName = entry.getHeader().getTableName();//表名
	            List<String> pkNames = getTablePkName(schemaName,tableName);
	            if(pkNames == null || pkNames.isEmpty()) {//如果是没有主键的表不进行操作
	            	continue;
	            }
//	            log.info(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",  
//												                    entry.getHeader().getLogfileName(), //日志文件名
//												                    entry.getHeader().getLogfileOffset(),//日志文件偏移量
//												                    schemaName,//数据库schema
//												                    tableName,//表名
//												                    eventType));//操作类型
	  
	            for (RowData rowData : rowChage.getRowDatasList()) {
	                if (eventType == EventType.DELETE) {//删除
	                	try {
							delQueue.offer((BinlogDeleteInfo)printColumn(rowData.getBeforeColumnsList(),0,schemaName,tableName,pkNames),100,TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	                } else if (eventType == EventType.INSERT) {//新增
	                	/*try {
							insertQueue.offer((BinlogInsertInfo)printColumn(rowData.getAfterColumnsList(),1,schemaName,tableName,pkNames),100,TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}  */
	                } else {//更新
	                    /*System.out.println("-------> before");  
	                    printColumn(rowData.getBeforeColumnsList());  
	                    System.out.println("-------> after");  */
	                	try {
							updateQueue.offer((BinlogUpdateInfo)printColumn(rowData.getAfterColumnsList(),2,schemaName,tableName,pkNames),100,TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}  
	                }  
	            } 
	        }  
	    }  
		
	/**
	 * 装载变更信息
	 * @param columns
	 * @param type
	 * @param schema
	 * @param tableName
	 * @param pkNames
	 * @return
	 */
    public  Object printColumn(@NotNull List<Column> columns,int type,String schema,String tableName,List<String> pkNames){
    	Object rs  = null;
    	if(type==0){//删除
    		BinlogDeleteInfo bean = new BinlogDeleteInfo();
    		Map<String,Object> pkMap = new HashMap<>();
    		List<Map<String,Object>> colList = new ArrayList<>();
    		Map<String,Object> col = null;
    		bean.setSchemaName(schema);
    		bean.setTableName(tableName);
    		for(Column c : columns) {
    			String colName = c.getName();
    			if(pkNames.contains(colName)) {//是主键
    				pkMap.put("name", colName);
    				pkMap.put("value", c.getValue());
    			}
    			col = new HashMap<>();
    			col.put("name",c.getName());
    			col.put("value",c.getValue());
    			colList.add(col);
    		}
    		bean.setPkMap(pkMap);
    		bean.setDeleteColList(colList);
    		rs = bean;
    	}else if(type == 1) {//新增
    		BinlogInsertInfo bean = new BinlogInsertInfo();
    		Map<String,Object> col = null;
    		bean.setSchemaName(schema);
    		bean.setTableName(tableName);
    		List<Map<String,Object>> colList = new ArrayList<>();
    		for(Column c : columns) {
    			col = new HashMap<>();
    			col.put("name",c.getName());
    			col.put("value",c.getValue());
    			colList.add(col);
    		}
    		bean.setInsertColList(colList);
    		rs = bean;
    	}else if(type == 2){//修改
    		BinlogUpdateInfo bean = new BinlogUpdateInfo();
    		Map<String,Object> col = null;
    		Map<String,Object> pkMap = new HashMap<>();
    		bean.setSchemaName(schema);
    		bean.setTableName(tableName);
    		List<Map<String,Object>> colList = new ArrayList<>();
    		for(Column c : columns) {
    			String colName = c.getName();
    			if(pkNames.contains(colName)) {//是主键
    				pkMap.put("name", colName);
    				pkMap.put("value", c.getValue());
    			}
//    			if(c.getUpdated()){//如果是被更新的字段
    				col = new HashMap<>();
	    			col.put("name",c.getName());
	    			col.put("value",c.getValue());
	    			colList.add(col);
//    			}
    			
    		}
    		bean.setPkMap(pkMap);
    		bean.setUpdateColList(colList);
    		rs = bean;
    	}
        return rs;
    }
	 
    /**
     * 获取表的主键名称
     * @param schema
     * @param tableName
     * @return
     */
    public List<String> getTablePkName(String schema,String tableName) {
    	List<String> rs = null;
    	MysqlJdbcComm jdbc = null;
    	try {
			jdbc = new MysqlJdbcComm(schema);
			rs = jdbc.getTablePk(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				jdbc.releaseConn();
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				jdbc = null;
			}
		}
    	return rs;
    }
}
