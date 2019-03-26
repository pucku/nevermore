package com.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertUtil {
	private static Logger logger = LoggerFactory.getLogger(AlertUtil.class);

	public static void alert(String info) {
		logger.warn(info);
	}
}
