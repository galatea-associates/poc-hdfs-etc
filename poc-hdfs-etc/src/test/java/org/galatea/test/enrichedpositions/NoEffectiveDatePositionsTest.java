package org.galatea.test.enrichedpositions;

import org.galatea.domain.input.Contract;
import org.galatea.domain.input.CounterParty;
import org.galatea.domain.input.Instrument;
import org.galatea.domain.input.Position;
import org.galatea.domain.result.EnrichedPositionsResults;
import org.galatea.util.SwapQueryTest;
import org.junit.Test;

public class NoEffectiveDatePositionsTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testEnrichedPositionsQuery() {
		new Contract().counterparty_id(200).swap_contract_id(12345).write();
		new CounterParty().counterparty_id(200).counterparty_field1("cp200Field1").write();
		new Instrument().instrument_id(11).ric("ABC").write();
		new Position().position_type("E").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();

		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResult(200, 20190102);

		results.assertResultCountEquals(0);
	}

}