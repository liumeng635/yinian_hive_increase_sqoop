package com.yinian.alysis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.Yinian2HiveUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbcYinian;

@SpringBootApplication
public class StartApplication {
	public static void main(String[] args) {
		SpringApplication.run(StartApplication.class, args);
		/*try {
			Yinian2HiveUtil util = new Yinian2HiveUtil();
			util.createListAllMysqlTable2Hive("yinian");
			util.syncMysqlData2Hive("yinian", true, null);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			MysqlJdbcYinian.getInstance().releaseConn();
			HiveJdbc.getInstance().releaseConn();
			RemoteShellTool.getInstance().releaseConn();
		}*/
	}
}