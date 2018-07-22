package com.yinian.alysis.canal.bean;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class HiveUpdateCacheRecd implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String schema;
	private String tableName;
	private String fileString;
	private Date createTime;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getFileString() {
		return fileString;
	}
	public void setFileString(String fileString) {
		this.fileString = fileString;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
}
