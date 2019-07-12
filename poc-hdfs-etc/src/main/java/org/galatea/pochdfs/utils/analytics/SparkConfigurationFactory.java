package org.galatea.pochdfs.utils.analytics;

import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;

public class SparkConfigurationFactory {

	@Value("${spark.home}")
	private static String sparkHome;

	private SparkConfigurationFactory() {
	}

	public static SparkConf newDefaultSparkConfiguration() {
		SparkConf sparkConfiguration = new SparkConf();
		sparkConfiguration.setSparkHome(sparkHome);
		sparkConfiguration.setMaster("yarn");
		sparkConfiguration.setAppName("spark-yarn");
		sparkConfiguration.set("master", "yarn");
		sparkConfiguration.set("spark.submit.deployMode", "cluster");
		return sparkConfiguration;
	}

}
