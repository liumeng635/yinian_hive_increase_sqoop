package com.yinian.alysis.config;

import java.io.File;
import java.io.IOException;

import com.yinian.alysis.exception.ExceptionHandle;
import com.yinian.alysis.util.PropertiesUtil;

public class MonitorCfg {
	/**
	 * quartz执行时间设置
	 */
	public static final String QUARZ_CRON_EXPRESSION = PropertiesUtil.getDataParam("quarz_cron_expression");
	public static void main(String[] args) {
		try {
			String tes = null;
			File file = new File(tes);
		} catch (Exception e) {
			System.out.println(ExceptionHandle.getErrorInfoFromException(e));
		}
	}
}
