package org.galatea.pochdfs.test.enrichedpositions;

import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.input.Instrument;
import org.galatea.pochdfs.domain.input.Position;
import org.galatea.pochdfs.domain.result.EnrichedPosition;
import org.galatea.pochdfs.domain.result.EnrichedPositionsResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class MultiplePositionsOnMultipleContracts extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testEnrichedPositionsQuery() {
		writeData();

		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(200, 20190101);

		results.assertResultCountEquals(3);

		results.assertHasEnrichedPosition(new EnrichedPosition().ric("ABC").effectiveDate(20190101).swapId(12345)
				.counterPartyField1("cp200Field1").instId(11).counterPartyId(200));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("DEF").effectiveDate(20190101).swapId(67890)
				.counterPartyField1("cp200Field1").instId(22).counterPartyId(200));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("HIJ").effectiveDate(20190101).swapId(67890)
				.counterPartyField1("cp200Field1").instId(33).counterPartyId(200));
	}

	private void writeData() {
		writeContracts();
		new CounterParty().counterparty_id(200).counterparty_field1("cp200Field1").write();
		writeInstruments();
		writePositions();
	}

	private void writeContracts() {
		new Contract().counterparty_id(200).swap_contract_id(12345).write();
		new Contract().counterparty_id(200).swap_contract_id(67890).write();
	}

	private void writeInstruments() {
		new Instrument().instrument_id(11).ric("ABC").write();
		new Instrument().instrument_id(22).ric("DEF").write();
		new Instrument().instrument_id(33).ric("HIJ").write();
	}

	private void writePositions() {
		new Position().position_type("S").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("I").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("E").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("S").swap_contract_id(67890).ric("DEF").effective_date(20190101).write();
		new Position().position_type("I").swap_contract_id(67890).ric("DEF").effective_date(20190101).write();
		new Position().position_type("E").swap_contract_id(67890).ric("DEF").effective_date(20190101).write();
		new Position().position_type("S").swap_contract_id(67890).ric("HIJ").effective_date(20190101).write();
		new Position().position_type("I").swap_contract_id(67890).ric("HIJ").effective_date(20190101).write();
		new Position().position_type("E").swap_contract_id(67890).ric("HIJ").effective_date(20190101).write();
	}

}