package org.galatea.pochdfs.domain.result;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnrichedPositionsResults {

	private final Dataset<Row> enrichedPositionsResults;

	public EnrichedPositionsResults(final Dataset<Row> enrichedPositionsResults) {
		this.enrichedPositionsResults = enrichedPositionsResults;
		log.info("Created EnrichedPositionsResults with {} records", enrichedPositionsResults.count());
		logResults();
	}

	public void assertResultCountEquals(final int expected) {
		assertEquals(expected, enrichedPositionsResults.count());
	}

	public void assertHasEnrichedPosition(final EnrichedPosition enrichedPosition) {
		log.info("Asserting enriched positions results has enriched position with data: {}", enrichedPosition);
		for (Row row : enrichedPositionsResults.collectAsList()) {
			if (enrichedPosition.equalsRow(row)) {
				return;
			}
		}
		Assert.fail("EnrichedPositions results does not contain enriched position with " + enrichedPosition);
	}

	private void logResults() {
		enrichedPositionsResults.foreach((row) -> {
			log.info(row.toString());
		});
	}

	public EnrichedPositionResult getSingleResult() {
		assertResultCountEquals(1);
		return new EnrichedPositionResult(enrichedPositionsResults.toLocalIterator().next());
	}

}
