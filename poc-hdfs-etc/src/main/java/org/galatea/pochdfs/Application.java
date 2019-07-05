package org.galatea.pochdfs;

import java.util.Scanner;

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
			while (!line.equalsIgnoreCase("quit")) {
				line = scanner.nextLine();
				analyzer.getEnrichedPositionsWithUnpaidCash(200, 20190103).show();
			}
		}
	}

}
