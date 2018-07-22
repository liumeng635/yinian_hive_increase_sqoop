package com.yinian.alysis.transData;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinianCount;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;
import com.yinian.alysis.util.PropertiesUtil;

public class YinainCount2HiveUtil {
	Logger log = Logger.getLogger(YinainCount2HiveUtil.class);
	public static String SEPARATOR = "/";
	public static void generateCreateTableSqlFiles(String schema,String path) throws Exception {
		MysqlJdbcYinianCount dbUtil =  MysqlJdbcYinianCount.getInstance();
		StringBuilder sb = new StringBuilder();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sb.append(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
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
	public List<String> generateCreateTableSqlList(String schema) throws Exception {
		MysqlJdbcYinianCount dbUtil = MysqlJdbcYinianCount.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<String> rs = new ArrayList<>();
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			if(!SyncCommUtil.checkIsPatitionTable(tableName)){//如果是这两张类型的表 不进行创建
				//如果是
				rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
			}
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
		MysqlJdbcYinianCount dbUtil = MysqlJdbcYinianCount.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(table);
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<String> rs = new ArrayList<>();
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
		}
		return rs;
	}
	
	/**
	 * 全部表创建datax json配置文件
	 * @Title: generateDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public List<Map<String,Object>> generateDataxJsonCfg(String dir,String schema) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String path = PropertiesUtil.getDataParam("datax.json.template");
		String temStr = FileUtils.readFileToString(new File(path), "UTF-8");
		MysqlJdbcYinianCount dbUtil = MysqlJdbcYinianCount.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		try {
			String tableName = "";
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				if(SyncCommUtil.checkIsPatitionTable(SyncCommUtil.trimTou(tableName))){//如果是定期分表的这类表 将数据合并到hive的一张表中
					if(SyncCommUtil.trimTou(tableName).indexOf("count_interface")>-1){
						tableName = "count_interface";
					}else if(SyncCommUtil.trimTou(tableName).indexOf("count_operation")>-1){
						tableName = "count_operation";
					}
				}
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", key);
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}", "");//全量同步的
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(key)+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", SyncCommUtil.trimTou(key));
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rsList;
	}
	
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
	public List<Map<String,Object>> generateTableDataxJsonCfg(String dir,String schema,String tableName) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String path = PropertiesUtil.getDataParam("datax.json.template");
		String temStr = FileUtils.readFileToString(new File(path), "UTF-8");
		MysqlJdbcYinianCount dbUtil = MysqlJdbcYinianCount.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				if(SyncCommUtil.checkIsPatitionTable(SyncCommUtil.trimTou(tableName))){//如果是定期分表的这类表 将数据合并到hive的一张表中
					if(SyncCommUtil.trimTou(tableName).indexOf("count_interface")>-1){
						tableName = "count_interface";
					}else if(SyncCommUtil.trimTou(tableName).indexOf("count_operation")>-1){
						tableName = "count_operation";
					}
				}
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", key);
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}",  SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}", "");//全量同步的
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(key)+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", SyncCommUtil.trimTou(key));
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
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
	public List<Map<String,Object>> generateTableIncreaseDataxJsonCfg(String dir,String schema,String tableName,Map<String,Object> syncTbInfo) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String path = PropertiesUtil.getDataParam("datax.json.template");
		String temStr = FileUtils.readFileToString(new File(path), "UTF-8");
		MysqlJdbcYinianCount dbUtil = MysqlJdbcYinianCount.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			
			
			
			String pkName = (String)syncTbInfo.get("pk_name");
			String lastMaxVal = (String)syncTbInfo.get("last_max_val");
			String condition = pkName+">'"+lastMaxVal+"'";
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				if(SyncCommUtil.checkIsPatitionTable(SyncCommUtil.trimTou(tableName))){//如果是定期分表的这类表 将数据合并到hive的一张表中
					if(SyncCommUtil.trimTou(tableName).indexOf("count_interface")>-1){
						tableName = "count_interface";
					}else if(SyncCommUtil.trimTou(tableName).indexOf("count_operation")>-1){
						tableName = "count_operation";
					}
				}
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", key);
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}",condition);
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(key)+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", SyncCommUtil.trimTou(key));
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
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
    	 //生成的文件拷贝到linux服务器上
    	 List<Map<String,Object>> jsonFiles =  null;
    	 if(all){
    		 jsonFiles = this.generateDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema);
    	 }else{
    		 jsonFiles = this.generateTableDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName);
    	 }
    	 if(!new File(PropertiesUtil.getDataParam("datax.local.path")).exists()){
    		 FileUtils.forceMkdir(new File(PropertiesUtil.getDataParam("datax.local.path")));
    	 }
    	 String now = System.currentTimeMillis()+"";
    	 String dir = PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now;
    	 boolean exec = tool.exec("mkdir "+dir);
    	 if(exec){
    		 MysqlJdbcYinianCount mysql = MysqlJdbcYinianCount.getInstance();
    		 try {
				SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
				 for(Map<String,Object> json : jsonFiles){
					 String jsonP = PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now+"/"+new File(String.valueOf(json.get("jobPath"))).getName();
					 String tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					 tool.transFile2Linux(String.valueOf(json.get("jobPath")), PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now);
					 Map<String,Object> valMap = null;
					 if(SyncCommUtil.checkIsPatitionTable(tableName)){
						 valMap = mysql.selectMaxCreateTime(tableName);
						 String recdIdMax = (String)valMap.get("val");
						 String pkName = (String)valMap.get("pkName");
						 hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
					 }else{
						 if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
							 valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
							 String recdIdMax = (String)valMap.get("val");
							 if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
								 recdIdMax = "0";
							 }
							 String pkName = (String)valMap.get("pkName");
							 hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
						 }
					 }
					 
					 log.info("正在同步表："+tableName+"的数据待hive上。。。。。。。。。.............start.............。。。。。。。。。");
					 tool.excuteDataxTransData(jsonP);
					 log.info("同步表："+tableName+"的数据待hive上完成。。。。。。。。。.............end.............。。。。。。。。。");
				 }
				 
				 //最后删除掉中间的过程文件
				 tool.exec("rm -rf "+dir);
				 FileUtils.cleanDirectory(new File(PropertiesUtil.getDataParam("datax.local.path")));
			} catch (Exception e) {
				e.printStackTrace();
			}
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
		MysqlJdbcYinianCount mysql = MysqlJdbcYinianCount.getInstance();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> jsonFiles =  null;
		//判断是否是有主键表 有增量 无全量
		if(mysql.hasPk(tabName)){//有主键增量同步
			jsonFiles = this.generateTableIncreaseDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName,syncInfo);
		}else{//无主键全量同步
			jsonFiles = this.generateTableDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName);
		}
		
		if(!new File(PropertiesUtil.getDataParam("datax.local.path")).exists()){
			FileUtils.forceMkdir(new File(PropertiesUtil.getDataParam("datax.local.path")));
		}
		String now = System.currentTimeMillis()+"";
		String dir = PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now;
		boolean exec = tool.exec("mkdir "+dir);
		if(exec){
			SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
			for(Map<String,Object> json : jsonFiles){
				String jsonP = PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
				String tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
				tool.transFile2Linux(String.valueOf(json.get("jobPath")), PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now);
				
				Map<String,Object> valMap = null;
				
				 if(SyncCommUtil.checkIsPatitionTable(tableName)){
					 valMap = mysql.selectMaxCreateTime(tableName);
					 String recdIdMax = (String)valMap.get("val");
					 String pkName = (String)valMap.get("pkName");
					 hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
				 }else{
					 if(mysql.hasPk(tableName)){
							valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
							String recdIdMax = String.valueOf(valMap.get("val"));
							if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
								recdIdMax = "0";
							}
							String pkName = (String)valMap.get("pkName");
							hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
					}else{//无主键  先删除再全量同步
						//将hive上表的数据删除掉
						HiveJdbc.getInstance().truncateTable(tabName,schema);
						//将hive在hdfs上的垃圾清除掉
						tool.truncateHiveHdfsRubish(tableName,schema);
					}
				 }
				 log.info("正在增量同步表："+tableName+"到hive上==================");
				 tool.excuteDataxTransData(jsonP);
				 log.info("增量同步到hive上结束==================");
			}
		}
		 //最后删除掉中间的过程文件
		 tool.exec("rm -rf "+dir);
		 FileUtils.cleanDirectory(new File(PropertiesUtil.getDataParam("datax.local.path")));
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
		List<String> list = this.generateCreateTableSqlList(schema);
		try {
			for(String sql : list){
				log.info(sql);
				HiveJdbc.getInstance().createTable(sql); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			HiveJdbc.getInstance().releaseConn();
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
		List<String> list = YinainCount2HiveUtil.generateCreateTableSql(schema,table);
		try {
			for(String sql : list){
				log.info(sql);
				HiveJdbc.getInstance().createTable(sql); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			HiveJdbc.getInstance().releaseConn();
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
	public void addSynMysql2Hive(String schema) throws Exception{
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = MysqlJdbcYinianCount.getInstance().findAllTables(schema);
		Map<String,Object> syncTbInfo = null;
		String tbName = null;
		String tbNameBak = null;
		for(Map<String, Object> tMap : allMysqlTables){
			tbName = (String)tMap.get("TABLE_NAME");
			tbNameBak = tbName;
			if(SyncCommUtil.trimTou(tbNameBak).indexOf("count_interface")>-1){
				tbNameBak = "count_interface";
			}else if(SyncCommUtil.trimTou(tbNameBak).indexOf("count_operation")>-1){
				tbNameBak = "count_operation";
			}
			
			if(HiveJdbc.getInstance().checkTableExists(tbNameBak,schema)){//如果存在
				syncTbInfo = SyncCommUtil.returnHiveTableSynInfo(tbName);
				//增量同步（有主键的增量去同步，无主键的先删除记录然后全量同步）
				this.syncMysqlIncreaseData2Hive(schema, tbName, syncTbInfo);
			}else{//如果不存在
				this.createMysqlTable2Hive(schema,tbNameBak);//创建hive表
				this.syncMysqlData2Hive(schema,false,tbName);//全量同步
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
//		YinainCount2HiveUtil util = new YinainCount2HiveUtil();
//		util.createListAllMysqlTable2Hive("yinian_count");
//		util.syncMysqlData2Hive("yinian_count", true, null);
//		util.addSynMysql2Hive("yinian_count");
//		MysqlJdbcYinianCount.getInstance().releaseConn();
//		HiveJdbc.getInstance().releaseConn();
//		RemoteShellTool.getInstance().releaseConn();
		clear();
	}
	
	public static void clear() throws SQLException{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc hive = HiveJdbc.getInstance();
		hive.excuteSql(" drop database yinian_count cascade");
		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian_count.db");
		hive.excuteSql(" create database yinian_count");
	}
}
