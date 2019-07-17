package org.galatea.pochdfs.domain.result;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Row;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EnrichedPositionResult {

	private final Row result;

	public void assertInstIdEquals(final int instId) {
		assertEquals(Long.valueOf(instId), result.getAs("instrument_id"));
	}

	public void assertSwapIdEquals(final int swapId) {
		assertEquals(Long.valueOf(swapId), result.getAs("swap_contract_id"));
	}

	public void assertRicEquals(final String ric) {
		assertEquals(ric, result.getAs("ric"));
	}

	public void assertCounterPartyIdEquals(final int counterPartyId) {
		assertEquals(Long.valueOf(counterPartyId), result.getAs("counterparty_id"));
	}

	public void assertCounterPartyFiel1Equals(final String value) {
		assertEquals(value, result.getAs("counterparty_field1"));
	}

	public void assertEffectiveDateEquals(final int effectiveDate) {
		assertEquals(Long.valueOf(effectiveDate), result.getAs("effective_date"));
	}

	public void assertTdQuantityEquals(final int tdQuantity) {
		assertEquals(Long.valueOf(tdQuantity), result.getAs("td_quantity"));
	}

}
