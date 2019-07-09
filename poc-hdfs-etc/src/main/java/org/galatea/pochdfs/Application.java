package org.galatea.pochdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.spark.HdfsAccessor;
import org.galatea.pochdfs.spark.SwapDataAnalyzer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {

	@SneakyThrows
	public static void main(final String[] args) {

		try (SparkSession sparkSession = SparkSession.builder().appName("SwapDataAccessor").getOrCreate()) {
			HdfsAccessor accessor = new HdfsAccessor(sparkSession);
			SwapDataAnalyzer analyzer = new SwapDataAnalyzer(accessor);
			Dataset<Row> enrichedPositionsWithUnpaidCash = analyzer.getEnrichedPositionsWithUnpaidCash(200, 20190103);
			enrichedPositionsWithUnpaidCash.show();
		}
	}

//	private static Dataset<Row> executeSql(final HdfsAccessor accessor, final String command) {
//		try {
//			return accessor.executeSql(command);
//		} catch (Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//	}

}
