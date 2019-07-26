package org.galatea.pochdfs.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SparkTestContext {

	private static transient SparkConf		CONF;
	private static transient SparkContext	CONTEXT;

	@BeforeClass
	public static void init() {
		CONF = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.ui.enabled", "false")
				.set("spark.driver.host", "localhost");
		CONTEXT = new SparkContext(CONF);
	}

	public static SparkContext getContext() {
		return CONTEXT;
	}

	@AfterClass
	public static void killSession() {

	}

}
