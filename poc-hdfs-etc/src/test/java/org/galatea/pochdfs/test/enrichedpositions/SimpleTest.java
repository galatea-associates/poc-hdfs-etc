package org.galatea.pochdfs.test.enrichedpositions;

import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.input.Instrument;
import org.galatea.pochdfs.domain.input.Position;
import org.galatea.pochdfs.domain.result.EnrichedPosition;
import org.galatea.pochdfs.domain.result.EnrichedPositionResult;
import org.galatea.pochdfs.domain.result.EnrichedPositionsResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class SimpleTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testEnrichedPositionsQuery() {
		// new Contract().counterparty_id(200).swap_contract_id(12345).write();
		Contract.defaultContract().write();
		new CounterParty().counterparty_id(200).counterparty_field1("cp200Field1").write();
		new Instrument().instrument_id(11).ric("ABC").write();
		new Position().position_type("S").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("I").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("E").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();

		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(200, 20190101);

		EnrichedPositionResult result = resultGetter.getSingleEnrichedPositionResult(200, 20190101);

		results.assertResultCountEquals(1);
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("ABC").effectiveDate(20190101).swapId(12345)
				.counterPartyField1("cp200Field1").instId(11).counterPartyId(Contract.DEFAULT_CONTRACT_ID));
	}

}
