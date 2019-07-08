package org.galatea.pochdfs;

import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.galatea.pochdfs.spark.HdfsAccessor;
import org.galatea.pochdfs.spark.SwapDataAnalyzer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {

	@SneakyThrows
	public static void main(final String[] args) {

		try (HdfsAccessor accessor = new HdfsAccessor(); Scanner scanner = new Scanner(System.in)) {
			SwapDataAnalyzer analyzer = new SwapDataAnalyzer(accessor);

			String line = "";

			Dataset<Row> unpaidCash = analyzer.getEnrichedPositionsWithUnpaidCash(200, 20190103);
			unpaidCash.show();
//			analyzer.getHdfsAccessor().createOrReplaceSqlTempView(unpaidCash, "unpaidCash");
//
//			while (!line.equalsIgnoreCase("quit")) {
//				try {
//					executeSql(analyzer.getHdfsAccessor(), line).show();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
			// line = scanner.nextLine();
//			}
		}
	}

	private static Dataset<Row> executeSql(final HdfsAccessor accessor, final String command) {
		try {
			return accessor.executeSql(command);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
