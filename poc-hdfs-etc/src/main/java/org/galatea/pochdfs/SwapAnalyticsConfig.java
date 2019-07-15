package org.galatea.pochdfs;

import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwapAnalyticsConfig {

//	@Bean
//	public SwapFilePathCreator filePathCreator() {
//		return SwapFilePathCreator.getInstance();
//	}

//	@Bean
//	public SwapDataAccessor swapDataAccessor() {
//		return new SwapDataAccessor(SparkSession.builder().appName("SwapDataAccessor").getOrCreate());
//	}

	@Bean
	@ConditionalOnProperty(value = "local.mode", havingValue = "false", matchIfMissing = true)
	public SparkSession sparkSession() {
		return SparkSession.builder().appName("SwapDataAnlyzer").getOrCreate();
	}

	@Bean
	@ConditionalOnProperty(value = "local.mode", havingValue = "true", matchIfMissing = true)
	public SwapDataAccessor swapDataAccessor() {
		return new SwapDataAccessor(null, null);
	}

}
