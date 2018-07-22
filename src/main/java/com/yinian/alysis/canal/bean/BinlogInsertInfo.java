package com.yinian.alysis.canal.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: BinlogInsertInfo 
 * @Description: binlog表记录插入信息描述对象
 * @author 刘猛
 * @date 2018年6月07日 上午9:19:05 
 *
 */
public class BinlogInsertInfo implements Serializable{
	private static final long serialVersionUID = 1L;
	private String schemaName;//数据库schema
	private String tableName;//表名
	private List<Map<String,Object>> insertColList;//插入字段信息
	 
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public List<Map<String, Object>> getInsertColList() {
		return insertColList;
	}
	public void setInsertColList(List<Map<String, Object>> insertColList) {
		this.insertColList = insertColList;
	}
	
	@Override
	public String toString() {
		return "插入======》》》》》》》》》》》》》 库表："+schemaName+"."+tableName+"\n                      字段信息："+insertColList+"\n----------------------------------------------------------------------------";
	}
}
