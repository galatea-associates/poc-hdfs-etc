package org.galatea.pochdfs;

import org.galatea.pochdfs.spark.SwapDataAccessor;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {

	@SneakyThrows
	public static void main(final String[] args) {

		try (SwapDataAccessor accessor = SwapDataAccessor.newDataAccessor()) {
			accessor.initializeDefaultSwapData();
			accessor.writeDataset(accessor.getEnrichedPositionsWithUnpaidCash(), "/test.json");
		}
	}

}
