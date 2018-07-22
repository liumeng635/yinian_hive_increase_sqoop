package com.yinian.alysis.tool;

import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.yinian.alysis.util.PropertiesUtil;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;

/** 
 * @ClassName: RemoteShellTool 
 * @Description:(远程连接linux服务器执行shell命令工具) 
 * @author 刘猛
 * @date 2018年6月2日 下午9:19:02
 */
public class RemoteShellTool {
	 private static Logger log = LoggerFactory.getLogger(RemoteShellTool.class);
	 private static Connection conn;    
     private static String ipAddr;    
     private String charset = "UTF-8";
     private static String userName;    
     private static String password;  
     public static RemoteShellTool tool;
     
     /*
      * 加载配置信息
      */
     static{
    	 loadConfig();
     }
     
     private RemoteShellTool() {  
    	  
     }
     
     /**
      * 获取实例
      * @return
      */
     public static RemoteShellTool getInstance(){
         if(tool == null){
             synchronized(RemoteShellTool.class){
                 if(tool == null){
                	 try {
						login();
					} catch (IOException e) {
						e.printStackTrace();
					}
                     tool = new RemoteShellTool();
                 }            
             }        
         }
         
         
         if(conn == null){
        	 synchronized(RemoteShellTool.class){
                 if(conn == null){
                	 try {
						login();
					} catch (IOException e) {
						e.printStackTrace();
					}
                 }            
             }   
         }
         return tool;
     }
     
     /**
      * 记载linux配置信息
      */
     public static void loadConfig() {  
         try {  
         	 ipAddr  = PropertiesUtil.getDataParam("remote_ssh_ip");  
         	 userName= PropertiesUtil.getDataParam("remote_ssh_user"); 
         	 password  = PropertiesUtil.getDataParam("remote_ssh_pwd");
         } catch (Exception e) {  
             throw new RuntimeException("读取数据库配置文件异常！", e);  
         }  
     }  
     
     /**
      * ssh登录
      * @Title: login 
      * @Description: TODO(这里用一句话描述这个方法的作用) 
      * @param @return
      * @param @throws IOException    设定文件 
      * @return boolean    返回类型 
      * @throws
      */
     private static boolean login() throws IOException {    
         try {
			 conn = new Connection(ipAddr);    
			 conn.connect(); // 连接    
		} catch (Exception e) {
			e.printStackTrace();
		}
         return conn.authenticateWithPassword(userName, password); // 认证    
     }    
     
     /**
      * 执行远程shell命令
      * @Title: exec 
      * @Description: TODO(这里用一句话描述这个方法的作用) 
      * @param @param cmds
      * @param @return    设定文件 
      * @return boolean    返回类型 
      * @throws
      */
     public boolean exec(String cmds) {    
         InputStream in = null;    
         String result = "";   
         boolean flag = false;  
         try {
        		 Session session = conn.openSession(); // 打开一个会话    
                 session.execCommand(cmds);    
                 in = session.getStdout();    
                 result = this.processStdout(in, this.charset);    
                 session.close();    
                 flag = true;
         } catch (IOException e1) {    
             e1.printStackTrace();    
         }
         log.info(result);
         return flag;    
     }    
     
    /**
     * 读取shell执行打印的信息
     * @param in
     * @param charset
     * @return
     */
     public String processStdout(InputStream in, String charset) {    
         byte[] buf = new byte[1024];    
         StringBuffer sb = new StringBuffer();    
         try {    
             while (in.read(buf) != -1) {    
                 sb.append(new String(buf, charset));    
             }    
         } catch (IOException e) {    
             e.printStackTrace();    
         }    
         return sb.toString();    
     }
     
     /**
      * 将本地文件上传到linux服务器上
      * @Title: transFile2Linux 
      * @Description: TODO(这里用一句话描述这个方法的作用) 
      * @param @param localPath
      * @param @param remotePath    设定文件 
      * @return void    返回类型 
      * @throws
      */
     public void transFile2Linux(String localPath,String remotePath){
 	    //文件scp到数据服务器  
 	    try {  
 	        SCPClient client = new SCPClient(conn);  
 	        client.put(localPath, remotePath); //本地文件scp到远程目录  
 	    } catch (IOException e) {  
 	        e.printStackTrace();  
 	    }
     }
     
     /**
      * 文件重linux服务器上下到本地
      * @Title: getFileFromLinux 
      * @Description: TODO(这里用一句话描述这个方法的作用) 
      * @param @param localPath
      * @param @param remotePath    设定文件 
      * @return void    返回类型 
      * @throws
      */
     public void getFileFromLinux(String localPath,String remotePath){
    	 //文件scp到数据服务器  
  	     try {
			SCPClient client = new SCPClient(conn);  
			client.get(remotePath, localPath);//远程的文件scp到本地目录  
		} catch (IOException e) {
			e.printStackTrace();
		}
     }
     
     /**
      * 删除hive表在hdfs上的垃圾
      * @Title: truncateHiveHdfsRubish 
      * @Description: TODO(这里用一句话描述这个方法的作用) 
      * @param @param tableName    设定文件 
      * @return void    返回类型 
      * @throws
      */
     public void truncateHiveHdfsRubish(String tableName,String schema){
    	 this.exec("hadoop fs -rmr .Trash/Current/user/hive/warehouse/"+schema+".db/"+tableName);
     }
     
     /**
      * 删除表数据
      * @param tableName
      * @param schema
      */
     public void truncateHiveTableData(String tableName,String schema) {
    	 this.exec("hadoop fs -rm -r  /user/hive/warehouse/"+schema+".db/"+tableName+"/*");
     }
     
     
     /**
      * 执行datax数据传输
      * @Title: excuteDataxTransData 
      * @Description: TODO(这里用一句话描述这个方法的作用) 
      * @param @param jsonPath    设定文件 
      * @return void    返回类型 
      * @throws
      */
     public boolean excuteDataxTransData(String jsonPath){
    	 return this.exec("python /root/datax/bin/datax.py "+jsonPath);
     }
     
     /**
      * 连接释放
      */
     public void releaseConn(){
    	 try {
			conn.close();
			conn = null;
		} catch (Exception e) {
			conn = null;
			e.printStackTrace();
		}
     }
     
     public static void main(String[] args) {
    	 RemoteShellTool tool1 = RemoteShellTool.getInstance();
//    	 String cmd = "sqoop import --connect jdbc:mysql://120.77.224.50:3306/yinian --username biuser --password 'i0Gkah5;uG' --query 'select * from `pictures` where 1=1 and $CONDITIONS' --target-dir /user/hive/warehouse/test.db/pictures/day=20180625/hour=11 --fields-terminated-by ',' -m 1";
    	 String cmd = "sqoop";
    	 tool1.exec(cmd);
    	 tool1.releaseConn();
	}
}
