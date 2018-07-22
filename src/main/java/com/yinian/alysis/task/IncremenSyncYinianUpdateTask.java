package com.yinian.alysis.task;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.YinianUpdate2HiveUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;
import com.yinian.alysis.util.PropertiesUtil;


/** 
 * @ClassName: ScheduleTask
 * @Description:监控定时任务 
 * @author 刘猛
 * @date 2018�?06�?04�? 下午2:48:00
 */
@Configuration  
@Component // 此注解必�?  
@EnableScheduling // 此注解必�? 
public class IncremenSyncYinianUpdateTask implements Job{
	private static final Logger LOGGER =  LoggerFactory.getLogger(IncremenSyncYinianUpdateTask.class);  
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		LOGGER.info("更新数据同步任务�?�?");
		YinianUpdate2HiveUtil util = new YinianUpdate2HiveUtil();
    	try {
    		LOGGER.info("�?始执�?");
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
	}
}
