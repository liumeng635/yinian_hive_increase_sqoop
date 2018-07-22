package com.yinian.alysis.transData.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

import com.yinian.alysis.util.PropertiesUtil;


public class MysqlJdbcYinianCount {
	// 表示定义数据库的用户名  
    private static String USERNAME ;  
  
    // 定义数据库的密码  
    private static String PASSWORD;  
  
    // 定义数据库的驱动信息  
    private static String DRIVER;  
  
    // 定义访问数据库的地址  
    private static String URL;  
  
    // 定义sql语句的执行对象  
    private PreparedStatement pstmt;  
  
    // 定义查询返回的结果集合  
    private ResultSet resultSet;
    
    private static Connection connection = null;
    //单利模式 --懒汉式(双重锁定)保证线程的安全性
    public static MysqlJdbcYinianCount db = null;
      
    static{  
        //加载数据库配置信息，并给相关的属性赋值  
        loadConfig();  
    }
    
    /** 
     * 加载数据库配置信息，并给相关的属性赋值 
     */  
    public static void loadConfig() {
        try {  
        	URL  = PropertiesUtil.getDataParam("bi.mysql.url.part")+PropertiesUtil.getDataParam("bi.mysql.schema.yinian_count");
        	USERNAME  = PropertiesUtil.getDataParam("bi.mysql.username");
        	PASSWORD  = PropertiesUtil.getDataParam("bi.mysql.password");
            DRIVER= PropertiesUtil.getDataParam("bi.mysql.driver");
        } catch (Exception e) {  
            throw new RuntimeException("读取数据库配置文件异常！", e);  
        }  
    }  
  
    private MysqlJdbcYinianCount() {  
  
    }
    
    public static MysqlJdbcYinianCount getInstance(){
        try {
			if(db == null){
			    synchronized(MysqlJdbcYinianCount.class){
			        if(db == null){
			        	init();
			            db = new MysqlJdbcYinianCount();
			        }            
			    }        
			}
			if(connection == null || connection.isClosed()){
				init();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return db;
    }
    
    
    private static void init(){
	 try {  
            Class.forName(DRIVER); // 注册驱动  
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD); // 获取连接  
        } catch (Exception e) {  
            throw new RuntimeException("get connection error!", e);  
        }  
    }
  
    /** 
     * 获取数据库连接 
     *  
     * @return 数据库连接 
     */  
    public Connection getConnection() {  
        return connection;  
    }  
  
    /** 
     * 执行更新操作 
     *  
     * @param sql 
     *            sql语句 
     * @param params 
     *            执行参数 
     * @return 执行结果 
     * @throws SQLException 
     */  
    public boolean updateByPreparedStatement(String sql, List<?> params)  
            throws SQLException {  
        boolean flag = false;  
        int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
        pstmt = connection.prepareStatement(sql);  
        int index = 1;  
        // 填充sql语句中的占位符  
        if (params != null && !params.isEmpty()) {  
            for (int i = 0; i < params.size(); i++) {  
                pstmt.setObject(index++, params.get(i));  
            }  
        }  
        result = pstmt.executeUpdate();  
        flag = result > 0 ? true : false;  
        return flag;  
    }
    
    /**
     * hive 建表
     * @Title: createTable 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param sql
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean createTable(String sql)  
            throws SQLException {  
        boolean flag = false;  
        int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
        pstmt = connection.prepareStatement(sql);  
        result = pstmt.executeUpdate();  
        flag = result > 0 ? true : false;  
        return flag;  
    } 
  
    /** 
     * 执行查询操作 
     *  
     * @param sql 
     *            sql语句 
     * @param params 
     *            执行参数 
     * @return 
     * @throws SQLException 
     */  
    public List<Map<String, Object>> findResult(String sql, List<?> params)  
            throws SQLException {  
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();  
        int index = 1;  
        pstmt = connection.prepareStatement(sql);  
        if (params != null && !params.isEmpty()) {  
            for (int i = 0; i < params.size(); i++) {  
                pstmt.setObject(index++, params.get(i));  
            }  
        }  
        resultSet = pstmt.executeQuery();  
        ResultSetMetaData metaData = resultSet.getMetaData();  
        int cols_len = metaData.getColumnCount();  
        while (resultSet.next()) {  
            Map<String, Object> map = new HashMap<String, Object>();  
            for (int i = 0; i < cols_len; i++) {  
                String cols_name = metaData.getColumnName(i + 1);  
                Object cols_value = resultSet.getObject(cols_name);  
                if (cols_value == null) {  
                    cols_value = "";  
                }  
                map.put(cols_name, cols_value);  
            }  
            list.add(map);  
        }  
        return list;  
    }  
  
    /** 
     * 释放资源 
     */  
    public void releaseConn() {  
        if (resultSet != null) {  
            try {  
                resultSet.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        if (pstmt != null) {  
            try {  
                pstmt.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        if (connection != null) {
            try {  
                connection.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
    }
    
    
    /**
     * 列出所有的表
     * @param schema
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> findAllTables(String schema) throws SQLException{
    	return  this.findResult("select table_name from information_schema.tables where TABLE_SCHEMA = '"+schema+"' and table_name <> 'count_split_table' ", null); 
    }
    
    /**
     * 获取hive建表相关信息
     * @Title: descTableTableStruct 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param conn
     * @param Exception设定文件 
     * @return Map<String,List<Map<String,Object>>>返回类型 
     * @throws
     */
    public  Map<String, List<Map<String,Object>>> descTableTableStruct() throws Exception{
    	DatabaseMetaData meta =  connection.getMetaData();
        ResultSet resultSet = meta.getTables(null,null, null, new String[] { "TABLE" });  
        Map<String,List<Map<String,Object>>> rsMap = new HashMap<>();
        while (resultSet.next()) {  
             String tableName=resultSet.getString("TABLE_NAME");
             if(StringUtils.equals("count_split_table", tableName)){continue;}
             ResultSet rs = connection.getMetaData().getColumns(null, getSchema(connection),tableName, "%");
             List<Map<String,Object>> colList = new ArrayList<>();
             while(rs.next()){  
                 Map<String,Object> map = new HashMap<String,Object>();  
                 String colName = rs.getString("COLUMN_NAME");
                 
                 map.put("code", "`"+colName+"`");  
                   
                 String remarks = rs.getString("REMARKS");  
                 if(remarks == null || remarks.equals("")){
                     remarks = colName;
                 }  
                 map.put("comment",remarks);  
                   
                 String dbType = rs.getString("TYPE_NAME");  
                 map.put("dbType",dbType);  
                   
                 map.put("valueType", changeDbType(dbType));  
                 colList.add(map);  
             }  
             rsMap.put("`"+tableName+"`", colList);
         }  
         
    	return  rsMap; 
    }
    
    /**
     * 单表表结构
     * @Title: descTableStruct 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tableName
     * @param @return
     * @param @throws Exception    设定文件 
     * @return Map<String,List<Map<String,Object>>>    返回类型 
     * @throws
     */
    public  Map<String,List<Map<String,Object>>> descTableStruct(String tableName) throws Exception{
    	DatabaseMetaData meta =  connection.getMetaData();
    	ResultSet resultSet = meta.getTables(null,"%", tableName, new String[] { "TABLE" });  
    	Map<String,List<Map<String,Object>>> rsMap = new HashMap<>();
    	while (resultSet.next()) {  
    		if(StringUtils.equals(resultSet.getString("TABLE_NAME"), tableName)){
    			ResultSet rs = connection.getMetaData().getColumns(null, getSchema(connection),tableName, "%");
        		List<Map<String,Object>> colList = new ArrayList<>();
        		while(rs.next()){  
        			Map<String,Object> map = new HashMap<String,Object>();  
        			String colName = rs.getString("COLUMN_NAME");
        			
        			map.put("code", "`"+colName+"`");  
        			
        			String remarks = rs.getString("REMARKS");  
        			if(remarks == null || remarks.equals("")){
        				remarks = colName;
        			}  
        			map.put("comment",remarks);  
        			
        			String dbType = rs.getString("TYPE_NAME");  
        			map.put("dbType",dbType);  
        			
        			map.put("valueType", changeDbType(dbType));  
        			colList.add(map);  
        		}  
        		rsMap.put("`"+tableName+"`", colList);
    		}
    		
    	}  
    	return  rsMap; 
    }
    
    /**
     * 查询最大id
     * @Title: selectPkIdMax 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tableName
     * @param @param conn
     * @param @throws SQLException
     * @param @throws Exception    设定文件 
     * @return void    返回类型 
     * @throws
     */
    public Map<String,Object> selectPkIdMax(String tableName) throws SQLException, Exception{
    		 Map<String,Object> rsMap = new HashMap<>();
             ResultSet pkRSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);//获取主键信息
             String pkName = "";
             if(pkRSet.next()){
            	 pkName = String.valueOf(pkRSet.getObject(4));//主键信息
             }
             String sql = "select max("+pkName+") as last_max_val from `"+tableName+"`";
            List<Map<String,Object>> maplist = this.findResult(sql,null);
            if(maplist == null || maplist.isEmpty()){
            	return null;
            }
            rsMap.put("pkName", pkName);
            String v = String.valueOf(maplist.get(0).get("last_max_val"));
            rsMap.put("val", v);
            return rsMap;
    }
    
    /**
     * 针对按照时间分表的表取创建时间增量同步
     * @Title: selectMaxCreateTime 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tableName
     * @param @return
     * @param @throws SQLException
     * @param @throws Exception    设定文件 
     * @return Map<String,Object>    返回类型 
     * @throws
     */
    public Map<String,Object> selectMaxCreateTime(String tableName) throws SQLException, Exception{
    	Map<String,Object> rsMap = new HashMap<>();
    	String sql = "select max(create_time) as last_max_val from `"+tableName+"`";
    	List<Map<String,Object>> maplist = this.findResult(sql,null);
    	if(maplist == null || maplist.isEmpty()){
    		return null;
    	}
    	rsMap.put("pkName", "create_time");
    	String v = String.valueOf(maplist.get(0).get("last_max_val"));
    	if(StringUtils.isEmpty(v) || StringUtils.equals("null", v)){
    		v = "0";
    	}
    	rsMap.put("val", v);
    	return rsMap;
    }
    
    
    
    /**
     * 判断表是否存在主键信息
     * @Title: hasPk 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tableName
     * @param @return    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean hasPk(String tableName){
    	boolean is = true;
        try {
			ResultSet pkRSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);//获取主键信息
			String pkName = "";
			if(pkRSet.next()){
			 	pkName = String.valueOf(pkRSet.getObject(4));//主键信息
			}
			if(StringUtils.isEmpty(pkName)){
				is = false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
       return is;
    }
    
    /**
     * 获取主键信息
     * @Title: getTablePk 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tableName
     * @param @return    设定文件 
     * @return String    返回类型 
     * @throws
     */
    public String getTablePk(String tableName){
    	String pkName = "";
    	try {
    		ResultSet pkRSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);//获取主键信息
    		if(pkRSet.next()){
    			pkName = String.valueOf(pkRSet.getObject(4));//主键信息
    		}
    	} catch (SQLException e) {
    		e.printStackTrace();
    	}
    	return pkName;
    }
    
    private static String changeDbType(String dbType) {  
        dbType = dbType.toUpperCase();  
        
        if(dbType.contains("CHAR")||dbType.contains("TEXT") || dbType.contains("VAR")){
        	return "string";
        }else if(dbType.contains("NUMBER") || dbType.contains("DECIMAL")){
        	return "decimal";
        }else if(dbType.contains("DOUBLE")){
        	return "double";
        }else if(dbType.contains("INT") || dbType.contains("BIT")){
        	if(dbType.contains("BIGINT")){
        		return "bigint";
        	}else{
        		return "int";
        	}
        }else if(dbType.contains("DATETIME") || dbType.contains("TIMESTAMP") || dbType.contains("DATE")){
//        	return "timestamp";
        	return "string";
        }else {
        	return "";
        }
        
    }
    
    private static String getSchema(Connection conn) throws Exception {  
        String schema;  
        schema = conn.getMetaData().getUserName();  
        if ((schema == null) || (schema.length() == 0)) {  
            throw new Exception("ORACLE数据库模式不允许为空");  
        }  
        return schema.toUpperCase().toString();  
  
    }  
}
