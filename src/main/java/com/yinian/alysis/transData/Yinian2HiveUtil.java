package com.yinian.alysis.transData;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import com.yinian.alysis.exception.ExceptionHandle;
import com.yinian.alysis.mail.SendMail;
import com.yinian.alysis.tool.GenerateHiveView;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;

public class Yinian2HiveUtil {
	private static Logger log = Logger.getLogger(Yinian2HiveUtil.class);
	
	public static String SEPARATOR = "/";
	
	public static void generateCreateTableSqlFiles(String schema,String path) throws Exception {
		MysqlJdbcYinian dbUtil =  MysqlJdbcYinian.getInstance();
		StringBuilder sb = new StringBuilder();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sb.append(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema)+"\n\n");
		}
		FileUtils.writeStringToFile(new File(path), sb.toString(), "UTF-8");
	}
	
	/**
	 * 所有表的hive创建sql
	 * @Title: generateCreateTableSqlList 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<String>    返回类型 
	 * @throws
	 */
	public static List<Map<String,String>> generateCreateTableSqlList(String schema) throws Exception {
		MysqlJdbcYinian dbUtil = MysqlJdbcYinian.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<Map<String,String>> rs = new ArrayList<Map<String,String>>();
		Map<String,String> sqlmap = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sqlmap = new HashMap<String,String>();
			sqlmap.put("sql", SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
			sqlmap.put("table", tableName);
			/*if(dbUtil.isContainUpdate(SyncCommUtil.trimTou(tableName))){
				rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(SyncCommUtil.trimTou(tableName)+"_update", listCol, schema));//更新从表  约定记录更新记录的从表后缀在主表基础上加上_update
			}*/
		}
		return rs;
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
			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
//			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(SyncCommUtil.trimTou(tableName)+"_update", listCol, schema));
		}
		return rs;
	}
	
	
	/**
	 * 全量同步sqoop的执行命令（table不是空单表同步）
	 * @param schema
	 * @param partions
	 * @return
	 * @throws Exception
	 */
	public static List<Map<String,Object>> generateSqoopCmd(String schema,String[] partions,String table) throws Exception{
		List<Map<String,Object>> rsList = new ArrayList<>();
		MysqlJdbcYinian dbUtil = MysqlJdbcYinian.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = null;
		if(StringUtils.isNotBlank(table)) {
			rsMap = dbUtil.descTableStruct(table);
		}else {
			rsMap = dbUtil.descTableTableStruct();
		}
		 
		try {
			String tableName = "";
			Map<String,Object> tMap = null;
			//组装分区信息
			String partition = "day="+partions[0]+"/hour="+partions[1];
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				tMap.put("tableName", tableName);
				tMap.put("cmd", SyncCommUtil.packageSqoopCmd(schema, tableName, "1=1", partition));//全量的同步
				rsList.add(tMap);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		return rsList;
	}
	
	/**
	 * 单表增量同步
	 * @Title: generateTableIncreaseDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @param tableName
	 * @param @param syncTbInfo
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateSqoopCmdIncrease(String schema,String tableName,Map<String,Object> syncTbInfo,String[] partions) throws Exception{
		List<Map<String,Object>> rsList = new ArrayList<>();
		MysqlJdbcYinian dbUtil = MysqlJdbcYinian.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			Map<String,Object> tMap = null;
			String partition = "day="+partions[0]+"/hour="+partions[1];
			String pkName = (String)syncTbInfo.get("pk_name");
			String lastMaxVal = (String)syncTbInfo.get("last_max_val");
			String condition = pkName+">"+lastMaxVal;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				tMap.put("tableName", tableName);
				tMap.put("cmd", SyncCommUtil.packageSqoopCmd(schema, tableName,condition, partition));
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
			sb1.append("{\"name\":\""+map.get("code")+"\",\"type\":\""+map.get("valueType")+"\"},");
			sb2.append("\""+map.get("code")+"\",");
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
	 * 将mysql表全量同步到hive上
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param hiveIpAddr
	 * @param @param hiveUser
	 * @param @param hivePwd
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void syncMysqlData2Hive(String schema,boolean all,String tabName) throws Exception{
		 RemoteShellTool tool = RemoteShellTool.getInstance();
		 HiveJdbc jdbc = HiveJdbc.getInstance();
		 String[] partition = SyncCommUtil.getNowDayAndHour();
    	 //生成的文件拷贝到linux服务器上
    	 List<Map<String,Object>> syncCmdlist =  null;
    	 if(all){//所有表全量同步
    		 syncCmdlist = Yinian2HiveUtil.generateSqoopCmd(schema,partition,null);
    	 }else{//单表全量同步
    		 syncCmdlist = Yinian2HiveUtil.generateSqoopCmd(schema,partition,tabName);
    	 }
    	
		 MysqlJdbcYinian mysql = MysqlJdbcYinian.getInstance();
		 try {
			SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
			String tableName = null;
			String cmd = null;
			 for(Map<String,Object> item : syncCmdlist){
				 tableName = SyncCommUtil.trimTou((String)item.get("tableName"));
				 cmd = (String)item.get("cmd");
				 if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
					 Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
					 String recdIdMax = String.valueOf(valMap.get("val"));
					 if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
							recdIdMax = "0";
					 }
					 String pkName = (String)valMap.get("pkName");
					 hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
				 }
				 if(mysql.isContainUpdate(tableName)) {//含有更新字段
					 hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
				 }
				 log.info("正在同步表："+tableName+"的数据到hive上。。。。。。。。。。。。。。。。。。");
				 log.info(cmd);
				 tool.exec(cmd);//执行同步
				 jdbc.excuteAddPartion(schema, tableName, partition);
				 log.info("同步表："+tableName+"的数据到hive上完成。。。。。。。。。。。。。。。。。。");
			 }
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
	}
	
	/**
	 * 新表不创建只是记录下同步信息
	 * @param tableName
	 * @param schema
	 * @throws Exception
	 */
	public static void recordNewTablesSync(String tableName,String schema) throws Exception {
		MysqlJdbcYinian mysql = MysqlJdbcYinian.getInstance();
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		if(mysql.isContainUpdate(tableName)) {
			hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
		}
		if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
			 String pkName = mysql.getTablePk(tableName);
			 hiveSync.saveOrUpdateMaxRecd(pkName, "0", tableName,schema);//记录下最大值0
		}
	}
	
	/**
	 * 单表增量导数
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param tabName
	 * @param @param syncInfo
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void syncMysqlIncreaseData2Hive(String schema,String tabName,Map<String,Object> syncInfo) throws Exception{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		MysqlJdbcYinian mysql = MysqlJdbcYinian.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		String[] partition = SyncCommUtil.getNowDayAndHour();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> syncCmdlist =  null;
		//判断是否是有主键表 有增量 无全量
		if(mysql.hasPk(tabName)){//如果有 增量
			syncCmdlist = Yinian2HiveUtil.generateSqoopCmdIncrease(schema,tabName,syncInfo,partition);
		}else{//全量同步
			syncCmdlist = Yinian2HiveUtil.generateSqoopCmd(schema,partition,tabName);
		}
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		String tableName = null;
		String cmd = null;
		for(Map<String, Object> json : syncCmdlist){
			tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
			cmd = (String)json.get("cmd");
			if(mysql.hasPk(tableName)){
				Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
				String recdIdMax = String.valueOf(valMap.get("val"));
				if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
					recdIdMax = "0";
				}
				String pkName = (String)valMap.get("pkName");
				hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
			}else{//无主键  先删除再全量同步
				//将hive上表的数据删除掉
				tool.truncateHiveHdfsRubish(tabName,schema);
				//将hive在hdfs上的垃圾清除掉
				tool.truncateHiveHdfsRubish(tableName,schema);
			}	
			log.info("正在增量同步表："+tableName+"到hive上==================");
			log.info(cmd);
			tool.exec(cmd);
			jdbc.excuteAddPartion(schema, tableName, partition);//创建分区
			log.info("增量同步到hive上结束==================");
		}
	}
	
	/**
	 * 将mysql所有表创建到hive上
	 * @Title: createListAllMysqlTable2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void createListAllMysqlTable2Hive(String schema) throws Exception{
		List<Map<String,String>> list = Yinian2HiveUtil.generateCreateTableSqlList(schema);
		HiveJdbc dbh = HiveJdbc.getInstance();
		MysqlJdbcYinian mysql = MysqlJdbcYinian.getInstance();
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		try {
			String sql = null;
			String tableName = null;
			 Map<String,Object> valMap = null;
			for(Map<String,String> sqMap : list){
				sql = sqMap.get("sql");
				tableName = sqMap.get("table");
				log.info(sql);
				dbh.createTable(tableName);
				GenerateHiveView.gerateView(schema,tableName);
				if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
					 valMap = mysql.selectPkIdMax(tableName);
					 String pkName = (String)valMap.get("pkName");
					 hiveSync.saveOrUpdateMaxRecd(pkName, "0", tableName,schema);//记录下最大值
				 }
				 if(mysql.isContainUpdate(tableName)) {//含有更新字段
					 hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
				 }
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
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
		List<String> list = Yinian2HiveUtil.generateCreateTableSql(schema,table);
		HiveJdbc dbh = HiveJdbc.getInstance();
		try {
			for(String sql : list){
				log.info(sql);
				dbh.createTable(sql);//建表
				GenerateHiveView.gerateView(schema,table);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
		}
	}
	
	/**
	 * 增量同步到hive
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
	public void addSynMysql2Hive(String schema){
		HiveJdbc hive = HiveJdbc.getInstance();
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = null;
		try {
			allMysqlTables = MysqlJdbcYinian.getInstance().findAllTables(schema);
		} catch (SQLException e1) {
			log.error(e1);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e1), null);
		}
		Map<String,Object> syncTbInfo = null;
		for(Map<String, Object> tMap : allMysqlTables){
			try {
				String tbName = (String)tMap.get("TABLE_NAME");
				if(hive.checkTableExists(tbName,schema)){//如果存在
					syncTbInfo = SyncCommUtil.returnHiveTableSynInfo(tbName);
					//增量同步（有主键的增量去同步，无主键的先删除记录然后全量同步）
					this.syncMysqlIncreaseData2Hive(schema, tbName, syncTbInfo);
				}else{//如果不存在
					this.createMysqlTable2Hive(schema,tbName);//创建hive表
					this.syncMysqlData2Hive(schema,false,tbName);//全量同步
					recordNewTablesSync(tbName, schema);//记录同步信息
				}
			} catch (SQLException e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Yinian2HiveUtil util = new Yinian2HiveUtil();
		util.createListAllMysqlTable2Hive("yinian");
//		util.syncMysqlData2Hive("yinian", true, null);
//		util.addSynMysql2Hive("yinian");
//		createSql2File("yinian", "D://createSql.txt");
//		clear();
		MysqlJdbcYinian.getInstance().releaseConn();
	}
	
	public static void clear() throws SQLException{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc hive = HiveJdbc.getInstance();
		hive.excuteSql(" drop database yinian cascade");
//		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
		tool.exec(" hadoop fs -rmr .Trash/");
		hive.excuteSql(" create database yinian");
	}
}
