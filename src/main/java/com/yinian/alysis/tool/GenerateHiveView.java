package com.yinian.alysis.tool;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.yinian.alysis.transData.SyncCommUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;

/** 
 * @ClassName: GenerateHiveView 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年6月12日 下午5:59:31
 */
public class GenerateHiveView {
	public static void main(String[] args) throws Exception {
		gerateView("yinian");
//		printNotContainUpdate();
//		removeTablePartitions();
	}
	
	public static void gerateView(String schema) throws Exception{
		HiveJdbc jdbc = HiveJdbc.getInstance();
		MysqlJdbcYinian yinian = MysqlJdbcYinian.getInstance();
    	List<Map<String, Object>> maplist = jdbc.findResult("show tables", null);
    	String sql = "";
    	for(Map map : maplist){
    		String tabName = (String)map.get("tab_name");
    		if(!StringUtils.endsWith(tabName,"_view")){//&& !StringUtils.endsWith(tabName,"_update")
    			if(yinian.isContainUpdate(tabName) && yinian.hasPk(tabName)) {//如果有主键和update字段则创建视图
    				sql = generateViewSql(tabName, schema,yinian.getTablePk(tabName));
    				System.out.println(sql);
        			jdbc.excuteSql(sql);
    			}
    		}
    	}
	}
	
	/**
	 * 单表创建视图
	 * @param schema
	 * @param tabName
	 * @throws Exception
	 */
	public static void gerateView(String schema,String tabName) throws Exception{
		HiveJdbc jdbc = HiveJdbc.getInstance();
		MysqlJdbcYinian yinian = MysqlJdbcYinian.getInstance();
		String sql = "";
			if(!StringUtils.endsWith(tabName,"_view")){//&& !StringUtils.endsWith(tabName,"_update")
				if(yinian.isContainUpdate(tabName) && yinian.hasPk(tabName)) {//如果有主键和update字段则创建视图
					sql = generateViewSql(tabName, schema,yinian.getTablePk(tabName));
					jdbc.excuteSql(sql);
			}
		}
	}
	
	/**
	 * @Title: generateViewSql 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param tableName
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static String generateViewSql(String tableName,String schema,String pkName){
		tableName = SyncCommUtil.trimTou(tableName);
		String tabName = schema+".`"+tableName+"`";
		String viewTabName = schema+"."+tableName+"_view";
		return "CREATE VIEW IF NOT EXISTS "+viewTabName+" as  select t1.* from "+tabName+" t1, (select "+pkName+",max(update_time) as update_time from "+tabName+" group by "+pkName+") t2 where t1."+pkName+" = t2."+pkName+" and t1.update_time = t2.update_time";
	}
	
	public static void printNotContainUpdate() {
		MysqlJdbcYinian jdbc = MysqlJdbcYinian.getInstance();
		try {
			List<Map<String, Object>>  list = jdbc.findAllTables("yinian");
			
			String tname = null;
			for(Map map : list) {
				tname = (String)map.get("TABLE_NAME");
				if(!jdbc.isContainUpdate(tname)) {
					System.out.println(tname);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void removeTablePartitions() throws IOException {
		try {
			HiveJdbc jdbc = HiveJdbc.getInstance();
			SyncInfoJdbc sync = SyncInfoJdbc.getInstance();
			List<String> list = FileUtils.readLines(new File("E:\\eclipse_workspace\\yinian_hive_increase\\src\\main\\resources\\table.txt"),"UTF-8");
			String table = null;
			for(String str : list){
				table = "`"+str+"`";
//				try {
//					jdbc.excuteSql("drop table "+table);
//					sync.resetMaxRecd(str, "yinian");
					String exc = "hadoop fs -rm -r /user/hive/warehouse/yinian.db/"+str+"/*";
					RemoteShellTool.getInstance().exec(exc);
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
