package com.yinian.alysis.transData.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yinian.alysis.transData.SyncCommUtil;
import com.yinian.alysis.util.PropertiesUtil;

public class HiveJdbc {
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
    public static HiveJdbc db = null;
      
    static{  
        //加载数据库配置信息，并给相关的属性赋值  
        loadConfig();  
    }
    
    /** 
     * 加载数据库配置信息，并给相关的属性赋值 
     */  
    public static void loadConfig() {  
        try {  
        	URL  = PropertiesUtil.getDataParam("hive.url");
        	USERNAME  = PropertiesUtil.getDataParam("hive.username");
            DRIVER= PropertiesUtil.getDataParam("hive.driver"); 
            PASSWORD  = PropertiesUtil.getDataParam("hive.password");
        } catch (Exception e) {  
            throw new RuntimeException("读取数据库配置文件异常！", e);  
        }  
    }  
  
    private HiveJdbc() {  
  
    }
    
    /**
     * 获取实例
     * @return
     */
    public static HiveJdbc getInstance(){
        if(db == null){
            synchronized(HiveJdbc.class){
                if(db == null){
                	init();
                    db = new HiveJdbc();
                }            
            }        
        }
        try {
			if(connection == null || connection.isClosed()){
				init();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return db;
    }
    
    /**
     * 初始化
     */
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
     * 删除表数据
     * @Title: truncateTable 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tabName
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean truncateTable(String tabName,String schema) throws SQLException{
    	 String sql = "truncate table "+schema+"."+tabName;
    	 boolean flag = false;  
         int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
         pstmt = connection.prepareStatement(sql);  
         result = pstmt.executeUpdate();  
         flag = result > 0 ? true : false;  
         return flag; 
    }
    
    /**
     * @Title: excuteSql 
     * @Description: 通用执行hive sql
     * @param @param sql
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean excuteSql(String sql) throws SQLException{
    	boolean flag = false;  
    	int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
    	pstmt = connection.prepareStatement(sql);  
    	result = pstmt.executeUpdate();  
    	flag = result > 0 ? true : false;  
    	return flag; 
    }
    
    public boolean excuteAddPartion(String schema,String tableName,String[] partition) throws SQLException{
    	tableName = SyncCommUtil.trimTou(tableName);
    	String sql = "alter table "+schema+".`"+tableName+"` add if not exists  partition (day='"+partition[0]+"',hour='"+partition[1]+"')";
    	boolean flag = false;  
    	int result = -1;
    	pstmt = connection.prepareStatement(sql);  
    	result = pstmt.executeUpdate();  
    	flag = result > 0 ? true : false;  
    	return flag; 
    }
    
    /**
     * @Title: excuteUpdate 
     * @Description: 执行更新操作
     * @param @param sql
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean excuteUpdate(String sql) throws SQLException{
    	boolean flag = false;  
    	int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
    	pstmt = connection.prepareStatement(sql);  
    	result = pstmt.executeUpdate();  
    	flag = result > 0 ? true : false;  
    	return flag; 
    }
    
    /**
     * 
     * @Title: excuteDeleteSql 
     * @Description: 执行删除操作
     * @param @param sql
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean excuteDeleteSql(String sql) throws SQLException{
    	boolean flag = false;  
    	int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
    	pstmt = connection.prepareStatement(sql);  
    	result = pstmt.executeUpdate();  
    	flag = result > 0 ? true : false;  
    	return flag; 
    }
    
    /**
     * 检测hive上的表是否已经创建
     * @Title: checkTableExists 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param tbname
     * @param @return    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean checkTableExists(String tbname,String schema){
    	boolean is = true;
    	String sql = "select 1 from "+schema+"."+tbname;
		try {
			 pstmt = connection.prepareStatement(sql);  
		     pstmt.executeUpdate();  
		} catch (SQLException e) {
			is = false;
		}
		return is;
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
}
