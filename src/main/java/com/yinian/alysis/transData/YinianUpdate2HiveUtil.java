package com.yinian.alysis.transData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import com.yinian.alysis.exception.ExceptionHandle;
import com.yinian.alysis.mail.SendMail;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;

public class YinianUpdate2HiveUtil {
	private static Logger log = Logger.getLogger(YinianUpdate2HiveUtil.class);
	
	public static String SEPARATOR = "/";
	
	/**
	 * 单表创建datax json配置文件
	 * @Title: generateTableDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateSqoopCmd(String schema,String tableName,Map<String,Object> syncInfo,String now,String[] partions) throws Exception{
		List<Map<String,Object>> rsList = new ArrayList<>();
		MysqlJdbcYinian dbUtil = MysqlJdbcYinian.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			Map<String,Object> tMap = null;
			String partition = "day="+partions[0]+"/hour="+partions[1];
			String colName = (String)syncInfo.get("basisc_col_name");
			String fetchTime = (String)syncInfo.get("last_fetch_time");
			String condition = colName + ">" + "'"+fetchTime+"' and "+colName+"<="+"'"+now+"' and create_time <> update_time";
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				tMap.put("tableName", tableName);
				tMap.put("jobPath", SyncCommUtil.packageSqoopCmd(schema, tableName, condition, partition));
				rsList.add(tMap);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		return rsList;
	}
	
	
	/**
	 * 生成mysql对应字段和hive字段信息
	 * @Title: generateHiveColInfo 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param cols
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static Map<String,String> generateHiveColInfo(List<Map<String, Object>> cols){
		StringBuilder sb1 = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		Map<String,String> rsMap = new HashMap<>();
		for(Map<String, Object> map : cols){
			sb1.append("{\"name\":\""+SyncCommUtil.trimTou(map.get("code"))+"\",\"type\":\""+map.get("valueType")+"\"},");
			sb2.append("\""+SyncCommUtil.trimTou(map.get("code"))+"\",");
		}
		String rs1 = sb1.toString();
		rs1 = rs1.substring(0,rs1.lastIndexOf(","));//mysql字段信息
		String rs2 = sb2.toString();
		rs2 = rs2.substring(0,rs2.lastIndexOf(","));//hive字段信息
		rsMap.put("mysql_cols", rs2);
		rsMap.put("hive_cols", rs1);
		return rsMap;
	}
	
	
	/**
	 * 单表增量导入更新数据
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param tabName
	 * @param @param syncInfo
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void syncMysqlUpdateData2Hive(String schema,String tabName,Map<String,Object> syncInfo) throws Exception{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		String[] partition = SyncCommUtil.getNowDayAndHour();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> syncCmdList =  null;
		String nowTime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
		//增量同步
		syncCmdList = YinianUpdate2HiveUtil.generateSqoopCmd(schema,tabName,syncInfo,nowTime,partition);
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		String tableName = null;//主表
		String cmd = null;
		for(Map<String, Object> item : syncCmdList){
			tableName = SyncCommUtil.trimTou((String)item.get("tableName"));
			cmd = (String)item.get("cmd");
			hiveSync.saveOrUpdateUpdateSyncInfo("update_time", nowTime, tableName, schema);
			log.info("正在同步更新数据："+tableName+"到hive上==================");
			log.info(cmd);
			tool.exec(cmd);
			jdbc.excuteAddPartion(schema, tableName, partition);//创建分区
			log.info("同步更新数据到到hive上结束==================");
		}
	}
	
	/**
	 * 增量同步更新数据到hive
	 * @Title: addSynMysql2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param hiveIpAddr
	 * @param @param hiveUser
	 * @param @param hivePwd
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void addSynUpdateDataMysql2Hive(String schema){
		MysqlJdbcYinian yinan = MysqlJdbcYinian.getInstance();
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = null;
		try {
			allMysqlTables = MysqlJdbcYinian.getInstance().findAllTables(schema);
		} catch (SQLException e1) {
			log.error(e1);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e1), null);
		}
		Map<String,Object> syncTbInfo = null;
		String tbName = "";
		for(Map<String, Object> tMap : allMysqlTables){
			tbName = (String)tMap.get("TABLE_NAME");
			try {
				if(!yinan.isContainUpdate(tbName) || !yinan.hasPk(tbName)){//不是含更新字段的表 或者是没有主键的表
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			
			/*if(!hive.checkTableExists(tbName,schema)){//更新表如果不存在
				this.createMysqlTable2Hive(schema,tbName);//创建hive表
			}*/
			//同步数据
			try {
				syncTbInfo = SyncCommUtil.returnHiveTUpdateableSynInfo(tbName,schema);
				if(syncTbInfo == null){
					return;
				}
				this.syncMysqlUpdateData2Hive(schema, tbName,syncTbInfo);
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
	
	/**
	 * mysql单表hive创建
	 * @Title: createMysqlTable2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param table
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void createMysqlTable2Hive(String schema,String table) throws Exception{
		List<String> list = YinianUpdate2HiveUtil.generateCreateTableSql(schema,table);
		HiveJdbc dbh = HiveJdbc.getInstance();
		try {
			for(String sql : list){
				log.info(sql);
				dbh.createTable(sql); 
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
		}
	}
	
	/**
	 * 单表hive创建sql
	 * @Title: generateCreateTableSql 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param table
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<String>    返回类型 
	 * @throws
	 */
	public static List<String> generateCreateTableSql(String schema,String table) throws Exception {
		MysqlJdbcYinian dbUtil = MysqlJdbcYinian.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(table);
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<String> rs = new ArrayList<>();
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(SyncCommUtil.trimTou(tableName), listCol, schema));
		}
		return rs;
	}
	
	public static void main(String[] args) throws Exception {
		YinianUpdate2HiveUtil util = new YinianUpdate2HiveUtil();
		util.addSynUpdateDataMysql2Hive("yinian");
		
		MysqlJdbcYinian.getInstance().releaseConn();
	}
	
	public static void clear() throws SQLException{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc hive = HiveJdbc.getInstance();
		hive.excuteSql(" drop database yinian cascade");
		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
		hive.excuteSql(" create database yinian");
	}
}
