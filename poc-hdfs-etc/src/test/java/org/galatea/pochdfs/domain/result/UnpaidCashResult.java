package org.galatea.pochdfs.domain.result;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Row;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UnpaidCashResult {

	private final Row result;

	public void assertInstIdEquals(final int instId) {
		assertEquals(Long.valueOf(instId), result.getAs("instrument_id"));
	}

	public void assertSwapIdEquals(final int swapId) {
		assertEquals(Long.valueOf(swapId), result.getAs("swap_contract_id"));
	}

	public void assertUnpaidDivEquals(final Double unpaidDiv) {
		assertEquals(unpaidDiv, result.getAs("unpaid_DIV"));
	}

	public void assertUnpaidIntEquals(final Double unpaidInt) {
		assertEquals(unpaidInt, result.getAs("unpaid_INT"));
	}

}