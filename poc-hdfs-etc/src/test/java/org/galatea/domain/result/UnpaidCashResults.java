package org.galatea.domain.result;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnpaidCashResults {

	private final Dataset<Row> unpaidCashRows;

	public UnpaidCashResults(final Dataset<Row> unpaidCashRows) {
		this.unpaidCashRows = unpaidCashRows;
		log.info("Created UnpaidCashResults with {} records", unpaidCashRows.count());
		logResults();
	}

	public void assertResultCountEquals(final int expected) {
		assertEquals(expected, unpaidCashRows.count());
	}

	public void assertHasCashflow(final UnpaidCash unpaidCash) {
		log.info("Asserting unpaid cash results has cashflow with data: {}", unpaidCash);
		for (Row row : unpaidCashRows.collectAsList()) {
			if (unpaidCash.equalsRow(row)) {
				return;
			}
		}
		Assert.fail("UnpaidCashResults does not contain cash flow with " + unpaidCash);
	}

	private void logResults() {
		unpaidCashRows.foreach((row) -> {
			log.info(row.toString());
		});
	}

}
