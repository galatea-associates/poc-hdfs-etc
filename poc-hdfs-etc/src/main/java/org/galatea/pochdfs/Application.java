package org.galatea.pochdfs;

import org.galatea.pochdfs.spark.HdfsAccessor;
import org.galatea.pochdfs.spark.SwapDataAnalyzer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {

	@SneakyThrows
	public static void main(final String[] args) {

		try (HdfsAccessor accessor = new HdfsAccessor()) {
			SwapDataAnalyzer analyzer = new SwapDataAnalyzer(accessor);
			analyzer.getEnrichedPositionsWithUnpaidCash(200, 20190103).show();
		}
	}

}
