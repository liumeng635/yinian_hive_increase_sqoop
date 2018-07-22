package com.yinian.alysis.canal.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;
import com.yinian.alysis.util.PropertiesUtil;


public class MysqlJdbcComm {
	// 表示定义数据库的用户名  
    private  String USERNAME ;  
  
    // 定义数据库的密码  
    private String PASSWORD;
  
    // 定义数据库的驱动信息  
    private String DRIVER;
  
    // 定义访问数据库的地址  
    private String URL;
  
    // 定义sql语句的执行对象  
    private PreparedStatement pstmt;
  
    // 定义查询返回的结果集合  
    private ResultSet resultSet;
    
    private static Connection connection = null;
    //单利模式 --懒汉式(双重锁定)保证线程的安全性
    public static MysqlJdbcYinian db = null;
    
    /** 
     * 加载数据库配置信息，并给相关的属性赋值 
     */  
    public void loadConfig(String schema) {  
        try {  
        	URL  = PropertiesUtil.getDataParam("bi.mysql.url.part")+schema;
        	USERNAME  = PropertiesUtil.getDataParam("bi.mysql.username");
        	PASSWORD  = PropertiesUtil.getDataParam("bi.mysql.password");
            DRIVER= PropertiesUtil.getDataParam("bi.mysql.driver");
        } catch (Exception e) {  
            throw new RuntimeException("读取数据库配置文件异常！", e);  
        }  
    }  
  
    public MysqlJdbcComm(String schema) {  
    	loadConfig(schema);
    	init();
    }
    
    
    private void init(){
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
    public List<String> getTablePk(String tableName){
    	List<String> rslist = new ArrayList<>();
    	String pkName = "";
    	try {
    		ResultSet pkRSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);//获取主键信息
    		while(pkRSet.next()){
    			pkName = String.valueOf(pkRSet.getObject(4));
    			if(StringUtils.isEmpty(pkName)) {continue;}
    			rslist.add(pkName);//主键信息
    		}
    	} catch (SQLException e) {
    		e.printStackTrace();
    	}
    	return rslist;
    }
    
    
    public static void main(String[] args) {
    	MysqlJdbcComm jdbc = new MysqlJdbcComm("yinian_count");
    	System.out.println(jdbc.getTablePk("count_split_table"));
	}
}
