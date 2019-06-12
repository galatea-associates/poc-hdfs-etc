package org.galatea.pochdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.galatea.pochdfs.spark.SwapDataAccessor;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {

	@SneakyThrows
	public static void main(final String[] args) {

		try (SwapDataAccessor accessor = SwapDataAccessor.newDataAccessor()) {
			accessor.initializeDefaultSwapData();
			Dataset<Row> dataset = accessor.getEnrichedPositions();
			dataset.show();
		}
	}

}
