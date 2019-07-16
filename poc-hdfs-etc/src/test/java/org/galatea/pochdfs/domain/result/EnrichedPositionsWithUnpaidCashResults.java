package org.galatea.pochdfs.domain.result;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnrichedPositionsWithUnpaidCashResults {

	private final Dataset<Row> results;

	public EnrichedPositionsWithUnpaidCashResults(final Dataset<Row> results) {
		this.results = results;
		log.info("Created EnrichedPositionsWithUnpaidCash with {} records", results.count());
		logResults();
	}

	public void assertResultCountEquals(final int expected) {
		assertEquals(expected, results.count());
	}

	public void assertHasEnrichedPositionWithUnpaidCash(final EnrichedPositionsWithUnpaidCash dataset) {
		log.info("Asserting enriched positions with unpaid cash results has position with unpaid cash with data: {}",
				dataset);
		for (Row row : results.collectAsList()) {
			if (dataset.equalsRow(row)) {
				return;
			}
		}
		Assert.fail("EnrichedPositionsWithUnpaidCash results does not contain record with " + dataset);
	}

	private void logResults() {
		results.foreach((row) -> {
			log.info(row.toString());
		});
	}

}