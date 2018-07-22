package com.yinian.alysis.transData;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;
import com.yinian.alysis.util.PropertiesUtil;

/** 
 * @ClassName: CommUtil 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年6月5日 下午1:43:10
 */
public class SyncCommUtil {
	public static String replaceFirstArg(String regx,String desStr,String content){
		 Pattern p = Pattern.compile(regx);
	     Matcher m = p.matcher(content);
	     String tmp = m.replaceFirst(desStr);
	     return tmp;
	}
	
	
	/**
	 * 查询上次同步的信息
	 * @Title: returnHiveTableSynInfo 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param table
	 * @param @return
	 * @param @throws SQLException    设定文件 
	 * @return Map<String,Object>    返回类型 
	 * @throws
	 */
	public static Map<String,Object> returnHiveTableSynInfo(String table) throws SQLException{
		List<Map<String,Object>> hiveTableList = SyncInfoJdbc.getInstance().listAllSynctTables();
		 Map<String,Object> rsMap = null;
		 for(Map<String,Object> map : hiveTableList){
			 String tableName = (String)map.get("table_name");
			 if(StringUtils.equals(table, tableName)){
				 rsMap = map;
				 break;
			 }
		 }
		return rsMap;
	}
	
	/**
	 * @Title: returnHiveTUpdateableSynInfo 
	 * @Description: 查询上次同步的更新信息
	 * @param @param table
	 * @param @return
	 * @param @throws SQLException    设定文件 
	 * @return Map<String,Object>    返回类型 
	 * @throws
	 */
	public static Map<String,Object> returnHiveTUpdateableSynInfo(String table,String schema) throws SQLException{
		List<Map<String,Object>> hiveTableList = SyncInfoJdbc.getInstance().listAllSyncUpdateTables(schema);
		Map<String,Object> rsMap = null;
		for(Map<String,Object> map : hiveTableList){
			String tableName = (String)map.get("table_name");
			if(StringUtils.equals(table, tableName) ){
				rsMap = map;
				break;
			}
		}
		return rsMap;
	}
	
	/**
	 * 去掉表名或字段`号
	 * @param o
	 * @return
	 */
	public static String trimTou(Object o){
		String str = String.valueOf(o);
		return StringUtils.isEmpty(str) ? "" : str.replaceAll("`","");
	}
	
	/**
	 * 埋点表的特例处理 判断是否是count_interface分表
	 * @param tableName
	 * @return
	 */
	public static boolean checkIsPatitionTable(String tableName){
		tableName = trimTou(tableName);
		
		boolean is = false;
		
		String str1 = tableName.replace("count_interface_", "");
		
		String str2 = tableName.replace("count_operation_", "");
		
		if(NumberUtils.isNumber(str1) || NumberUtils.isNumber(str2)){
			is = true;
		}
		return is;
	}
	
	
	/**
	 * 
	 * @Title: gerateCreateTableStoreAsTextSql 
	 * @Description: 建表sql（是以textfile存储）
	 * @param @param tableName
	 * @param @param list
	 * @param @param schema
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static String gerateCreateTableStoreAsTextSql(String tableName,List<Map<String,Object>> list,String schema) {
		String sql = "CREATE EXTERNAL TABLE IF NOT EXISTS "+schema+"."+tableName+" (\n";
		for(int i=0;i<list.size();i++) {
//			if(i<list.size()-1) {
				sql += "	"+list.get(i).get("code")+" "+ list.get(i).get("valueType")+",\n";
			/*}else {
				sql += "	"+list.get(i).get("code")+" "+ list.get(i).get("valueType")+"\n";
			}*/
		}
		sql += "    `isDel` int\n";
		sql += " )\n";
		sql += " partitioned by (day string, hour string)\n";
		
		
		sql += " row format delimited fields terminated by ','\n";
		sql += " STORED AS textfile\n\n";
		
//		sql += " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n";
//		sql += " with serdeproperties('serialization.null.format' = '','field.delim'=',')\n";
//		sql += " STORED AS ORC\n\n";
		return sql;
	}
	
	/**
	 * @Title: gerateCreateTableStoreAsOrcSql 
	 * @Description: 建表以orc格式存储
	 * @param @param tableName
	 * @param @param list
	 * @param @param schema
	 * @param @param pkName
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static String gerateCreateTableStoreAsOrcSql(String tableName,List<Map<String,Object>> list,String schema,String pkName) {
		String clusteredId = "";
		if(StringUtils.isEmpty(pkName)){
			clusteredId = (String)list.get(0).get("code");
		}else{
			clusteredId = pkName;
		}
		String sql = "CREATE TABLE IF NOT EXISTS "+schema+"."+tableName+" (";
		for(int i=0;i<list.size();i++) {
			if(i<list.size()-1) {
				sql += "	"+list.get(i).get("code")+" "+ list.get(i).get("valueType")+",";
			}else {
				sql += "	"+list.get(i).get("code")+" "+ list.get(i).get("valueType");
			}
		}
		sql +=  " )";
		sql += "clustered by ("+clusteredId+") into 4 buckets ";
		sql += " row format delimited fields terminated by ','";
		sql += " stored as orc TBLPROPERTIES('transactional'='true')";
		return sql;
	}
	
	
	/**
     * 组装sqoop命令
     * @param schema
     * @param table
     * @param conditin
     * @param partition
     * @return
     */
    public static String packageSqoopCmd(String schema,String table,String condition,String partition) {
   	 StringBuilder sb  = new StringBuilder();
   	 sb.append("sqoop import ");
   	 sb.append("--connect "+"jdbc:mysql://"+PropertiesUtil.getDataParam("bi.mysql.url.inner.ip")+":3306/"+schema+" ");
   	 sb.append("--username "+PropertiesUtil.getDataParam("bi.mysql.username")+" ");
   	 sb.append("--password '"+PropertiesUtil.getDataParam("bi.mysql.password")+"' ");
   	 sb.append("--query 'select * from "+table+" where "+condition+" and $CONDITIONS' ");
   	 sb.append("--target-dir /user/hive/warehouse/"+schema+".db/"+SyncCommUtil.trimTou(table)+"/"+partition+" ");
   	 sb.append("--fields-terminated-by ',' -m 1");
   	 return sb.toString();
    }
	
	public static String[] getNowDayAndHour() {
		SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd:HH");
		String now = sf.format(new Date());
		return now.split(":");
	}
	
	public static void main(String[] args) {
		System.out.println(checkIsPatitionTable("count_operation"));
	}
	
}
