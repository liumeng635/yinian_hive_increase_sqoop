package com.yinian.alysis.config.Schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.yinian.alysis.exception.ExceptionHandle;
import com.yinian.alysis.mail.SendMail;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.Yinian2HiveUtil;
import com.yinian.alysis.transData.YinianUpdate2HiveUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;
import com.yinian.alysis.util.PropertiesUtil;

@Component
public class SyncQuartz {
	private static final Logger LOGGER =  LoggerFactory.getLogger(SyncQuartz.class);
  	@Scheduled(cron = "0 0 0/1 * * ?") // 每小时执行一次
    public void work() throws Exception {
  		
  		new Thread(new Runnable() {
			@Override
			public void run() {
				LOGGER.info("增量同步任务开始");
				Yinian2HiveUtil util2 = new Yinian2HiveUtil();
		    	try {
		    		LOGGER.info("开始执行");
					util2.addSynMysql2Hive(PropertiesUtil.getDataParam("bi.mysql.schema.yinian"));
					LOGGER.info("执行结束");
				} catch (Exception e) {
					e.printStackTrace();
					LOGGER.error("业务数据同步出现故障",e);
					SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
				}finally {
					MysqlJdbcYinian.getInstance().releaseConn();
					HiveJdbc.getInstance().releaseConn();
					RemoteShellTool.getInstance().releaseConn();
				}
			}
		}).start();
  		
  		
  		new Thread(new Runnable() {
  			@Override
  			public void run() {
  				LOGGER.info("更新数据同步任务开始");
  				YinianUpdate2HiveUtil util = new YinianUpdate2HiveUtil();
  		    	try {
  		    		LOGGER.info("开始执行");
  		    		util.addSynUpdateDataMysql2Hive(PropertiesUtil.getDataParam("bi.mysql.schema.yinian"));
  		    		LOGGER.info("执行结束");
  				} catch (Exception e) {
  					e.printStackTrace();
  					LOGGER.error("业务数据同步出现故障",e);
  					SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
  				}finally {
  					MysqlJdbcYinian.getInstance().releaseConn();
  					HiveJdbc.getInstance().releaseConn();
  					RemoteShellTool.getInstance().releaseConn();
  				}
  			}
  		}).start();
    }
  	
//  	@Scheduled(cron = "*/5 * * * * ?") // 每小时执行一次
    /*public void work1() throws Exception {
  		LOGGER.info("更新数据同步任务开始");
		YinianUpdate2HiveUtil util = new YinianUpdate2HiveUtil();
    	try {
    		LOGGER.info("开始执行");
    		util.addSynUpdateDataMysql2Hive(PropertiesUtil.getDataParam("bi.mysql.schema.yinian"));
    		LOGGER.info("执行结束");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("业务数据同步出现故障",e);
		}finally {
			MysqlJdbcYinian.getInstance().releaseConn();
			HiveJdbc.getInstance().releaseConn();
			RemoteShellTool.getInstance().releaseConn();
		}
    }*/
}
