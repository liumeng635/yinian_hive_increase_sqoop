package com.yinian.alysis.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertiesUtil {
	//配置文件
		private static final String SYS_PROPERTIES = "properties/sys.static.variable.properties";
		public static Properties props = null;

		public static Properties getDataProps() {
			return props;
		}

		private static synchronized void getInstance() {
			props = new Properties();

			try {
				InputStreamReader inputStream = new InputStreamReader(Thread.currentThread()
						.getContextClassLoader().getResourceAsStream(SYS_PROPERTIES),"UTF-8");
				props.load(inputStream);
			} catch (FileNotFoundException e2) {
			} catch (IOException e1) {
			} catch (Exception e) {
			}
		}
		
		/**
		 * 后去参数名对应的参数值
		 * @param name dqms.properties文件中的参数名
		 * @return
		 */
		public static String getDataParam(String name){
			if (props == null){
				getInstance();
			}
			return props.getProperty(name);
		}
		
}
