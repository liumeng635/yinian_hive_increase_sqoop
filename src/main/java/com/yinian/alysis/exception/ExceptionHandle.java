package com.yinian.alysis.exception;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionHandle {
	public static String getErrorInfoFromException(Exception e) {
		StringWriter sw = null;
        PrintWriter pw = null;
        try {
            sw = new StringWriter();
            pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "\r\n" + sw.toString() + "\r\n";
        } catch (Exception e2) {
            return "ErrorInfoFromException";
        }finally {
        	try {
				sw.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
            pw.close();
		}
    }
}
