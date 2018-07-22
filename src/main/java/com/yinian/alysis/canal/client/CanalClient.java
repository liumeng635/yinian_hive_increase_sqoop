package com.yinian.alysis.canal.client;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;  

/**
 * canal客户端
 * @ClassName: CanalClient 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年6月10日 下午6:39:17 
 *
 */
public class CanalClient {
	Logger log = LoggerFactory.getLogger(CanalClient.class);
	private static CanalConnector connector = null;
	private static CanalClient client = null;
	private static String IP;
	private static int PORT;
	private static String DESTINATION;
	private static String USERNAME;
	private static String PASSWORD;
	static {
		loadConfig();
	}
	private CanalClient() {  
		  
	}
	 
	public CanalConnector getConnector() {
		return connector;
	}
	
	/**
	 * 获取实例
	 * @return
	 */
    public static CanalClient getInstance(){
        if(client == null){
            synchronized(CanalClient.class){
                if(client == null){
                	init();
                	client = new CanalClient();
                }            
            }        
        }
        if(connector == null || !connector.checkValid()){//连接为空或者连将失效
			init();
		}
        return client;
    }
	
	private static void loadConfig() {
		try {
			IP="120.79.107.83";
			PORT=22222;
			DESTINATION="example";
			USERNAME="";
			PASSWORD="";
		} catch (Exception e) {
			throw new RuntimeException("读取canal配置文件异常！", e);  
		}
	}
	
	
	 /**
     * 初始化
     */
    private static void init(){
	 try {  
		 // 创建链接  
	     connector = CanalConnectors.newSingleConnector(new InetSocketAddress(IP,PORT), DESTINATION, USERNAME, PASSWORD);
        } catch (Exception e) {  
            throw new RuntimeException("get connection error!", e);  
        }  
    }
}
