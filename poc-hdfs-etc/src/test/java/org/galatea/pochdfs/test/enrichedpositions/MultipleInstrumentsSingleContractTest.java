package org.galatea.pochdfs.test.enrichedpositions;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.input.Instrument;
import org.galatea.pochdfs.domain.input.Position;
import org.galatea.pochdfs.domain.result.EnrichedPosition;
import org.galatea.pochdfs.domain.result.EnrichedPositionsResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultipleInstrumentsSingleContractTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;

	private static final String	INST_1_RIC			= "ABC";
	private static final int	INST_1_ID			= 11;
	private static final int	INST_1_S_TD_QTY		= 100;

	private static final String	INST_2_RIC			= "DEF";
	private static final int	INST_2_ID			= 22;
	private static final int	INST_2_S_TD_QTY		= 25;

	private static final String	INST_3_RIC			= "GHI";
	private static final int	INST_3_ID			= 33;
	private static final int	INST_3_S_TD_QTY		= 15;

	@BeforeClass
	public static void writeData() {
		Contract.defaultContract().write();
		CounterParty.defaultCounterParty().write();
		writeInstruments();
		writePositions();
	}

	private static void writeInstruments() {
		Instrument.defaultInstrument().ric(INST_1_RIC).instrument_id(INST_1_ID).write();
		Instrument.defaultInstrument().ric(INST_2_RIC).instrument_id(INST_2_ID).write();
		Instrument.defaultInstrument().ric(INST_3_RIC).instrument_id(INST_3_ID).write();
	}

	private static void writePositions() {
		Position.defaultPosition().ric(INST_1_RIC).position_type("S").td_quantity(INST_1_S_TD_QTY).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("E").td_quantity(50).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("I").td_quantity(75).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("S").td_quantity(INST_2_S_TD_QTY).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("E").td_quantity(50).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("I").td_quantity(75).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("S").td_quantity(INST_3_S_TD_QTY).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("E").td_quantity(50).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("I").td_quantity(75).write();
	}

	@Test
	public void testEnrichedPositionsQuery() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE);

		results.assertResultCountEquals(3);
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_1_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(Defaults.CONTRACT_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_1_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_1_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_2_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(Defaults.CONTRACT_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_2_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_2_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_3_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(Defaults.CONTRACT_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_3_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_3_S_TD_QTY).book(Defaults.BOOK));
	}

	@Test
	public void testNoEffectiveDatePositions() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE + 1);
		results.assertResultCountEquals(0);
	}

	@Test
	public void testNoCounterParty() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults("missingBook",
				Defaults.EFFECTIVE_DATE);
		results.assertResultCountEquals(0);
	}

}
