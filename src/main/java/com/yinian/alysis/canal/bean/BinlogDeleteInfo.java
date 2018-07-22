package com.yinian.alysis.canal.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: BinlogDeleteInfo 
 * @Description: binlog表记录删除信息描述对象
 * @date 2018年6月07日 上午9:19:05 
 *
 */
public class BinlogDeleteInfo implements Serializable{
	private static final long serialVersionUID = 1L;
	private String schemaName;//数据库schema
	private String tableName;//表名
	private Map<String,Object> pkMap;//主键信息
	private List<Map<String,Object>> deleteColList;//字段删除信息
	
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
	public Map<String, Object> getPkMap() {
		return pkMap;
	}
	public void setPkMap(Map<String, Object> pkMap) {
		this.pkMap = pkMap;
	}
	public List<Map<String, Object>> getDeleteColList() {
		return deleteColList;
	}
	public void setDeleteColList(List<Map<String, Object>> deleteColList) {
		this.deleteColList = deleteColList;
	}
	@Override
	public String toString() {
		return "删除======》》》》》》》》》》》》》 库表："+schemaName+"."+tableName+"\n                      主键信息："+pkMap+"\n----------------------------------------------------------------------------";
	}
}
