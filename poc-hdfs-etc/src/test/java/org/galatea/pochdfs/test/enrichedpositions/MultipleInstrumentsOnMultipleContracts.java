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

public class MultipleInstrumentsOnMultipleContracts extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;

	private static final int	CONTRACT_1_ID		= 12345;
	private static final int	CONTRACT_2_ID		= 67890;
	private static final int	CONTRACT_3_ID		= 54321;

	private static final String	INST_1_RIC			= "ABC";
	private static final int	INST_1_ID			= 11;
	private static final int	INST_1_S_TD_QTY		= 100;

	private static final String	INST_2_RIC			= "DEF";
	private static final int	INST_2_ID			= 22;
	private static final int	INST_2_S_TD_QTY		= 70;

	private static final String	INST_3_RIC			= "GHI";
	private static final int	INST_3_ID			= 33;
	private static final int	INST_3_S_TD_QTY		= 40;

	private static final String	INST_4_RIC			= "JKL";
	private static final int	INST_4_ID			= 44;
	private static final int	INST_4_S_TD_QTY		= 95;

	private static final String	INST_5_RIC			= "MNO";
	private static final int	INST_5_ID			= 55;
	private static final int	INST_5_S_TD_QTY		= 65;

	private static final String	INST_6_RIC			= "PQR";
	private static final int	INST_6_ID			= 66;
	private static final int	INST_6_S_TD_QTY		= 35;

	private static final String	INST_7_RIC			= "STU";
	private static final int	INST_7_ID			= 77;
	private static final int	INST_7_S_TD_QTY		= 97;

	private static final String	INST_8_RIC			= "VWX";
	private static final int	INST_8_ID			= 88;
	private static final int	INST_8_S_TD_QTY		= 67;

	private static final String	INST_9_RIC			= "YZA";
	private static final int	INST_9_ID			= 99;
	private static final int	INST_9_S_TD_QTY		= 37;

	@BeforeClass
	public static void writeData() {
		writeContracts();
		CounterParty.defaultCounterParty().write();
		writeInstruments();
		writePositions();
	}

	private static void writeContracts() {
		Contract.defaultContract().swap_contract_id(12345).write();
		Contract.defaultContract().swap_contract_id(67890).write();
		Contract.defaultContract().swap_contract_id(54321).write();
	}

	private static void writeInstruments() {
		Instrument.defaultInstrument().ric(INST_1_RIC).instrument_id(INST_1_ID).write();
		Instrument.defaultInstrument().ric(INST_2_RIC).instrument_id(INST_2_ID).write();
		Instrument.defaultInstrument().ric(INST_3_RIC).instrument_id(INST_3_ID).write();
		Instrument.defaultInstrument().ric(INST_4_RIC).instrument_id(INST_4_ID).write();
		Instrument.defaultInstrument().ric(INST_5_RIC).instrument_id(INST_5_ID).write();
		Instrument.defaultInstrument().ric(INST_6_RIC).instrument_id(INST_6_ID).write();
		Instrument.defaultInstrument().ric(INST_7_RIC).instrument_id(INST_7_ID).write();
		Instrument.defaultInstrument().ric(INST_8_RIC).instrument_id(INST_8_ID).write();
		Instrument.defaultInstrument().ric(INST_9_RIC).instrument_id(INST_9_ID).write();
	}

	private static void writePositions() {
		writeInst1Positions();
		writeInst2Positions();
		writeInst3Positions();
		writeInst4Positions();
		writeInst5Positions();
		writeInst6Positions();
		writeInst7Positions();
		writeInst8Positions();
		writeInst9Positions();
	}

	@Test
	public void testEnrichedPositionsQuery() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE);

		results.assertResultCountEquals(9);

		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_1_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_1_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_1_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_2_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_2_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_2_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_3_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_3_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_3_S_TD_QTY).book(Defaults.BOOK));

		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_4_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_2_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_4_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_4_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_5_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_2_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_5_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_5_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_6_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_2_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_6_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_6_S_TD_QTY).book(Defaults.BOOK));

		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_7_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_3_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_7_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_7_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_8_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_3_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_8_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_8_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_9_RIC).effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(CONTRACT_3_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_9_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_9_S_TD_QTY).book(Defaults.BOOK));
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

	private static void writeInst1Positions() {
		Position.defaultPosition().ric(INST_1_RIC).position_type("S").td_quantity(INST_1_S_TD_QTY)
				.swap_contract_id(12345).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("E").td_quantity(90).swap_contract_id(CONTRACT_1_ID)
				.write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("I").td_quantity(80).swap_contract_id(CONTRACT_1_ID)
				.write();
	}

	private static void writeInst2Positions() {
		Position.defaultPosition().ric(INST_2_RIC).position_type("S").td_quantity(INST_2_S_TD_QTY)
				.swap_contract_id(12345).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("E").td_quantity(60).swap_contract_id(CONTRACT_1_ID)
				.write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("I").td_quantity(50).swap_contract_id(CONTRACT_1_ID)
				.write();
	}

	private static void writeInst3Positions() {
		Position.defaultPosition().ric(INST_3_RIC).position_type("S").td_quantity(INST_3_S_TD_QTY)
				.swap_contract_id(12345).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("E").td_quantity(30).swap_contract_id(CONTRACT_1_ID)
				.write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("I").td_quantity(20).swap_contract_id(CONTRACT_1_ID)
				.write();
	}

	private static void writeInst4Positions() {
		Position.defaultPosition().ric(INST_4_RIC).position_type("S").td_quantity(INST_4_S_TD_QTY)
				.swap_contract_id(67890).write();
		Position.defaultPosition().ric(INST_4_RIC).position_type("E").td_quantity(85).swap_contract_id(CONTRACT_2_ID)
				.write();
		Position.defaultPosition().ric(INST_4_RIC).position_type("I").td_quantity(75).swap_contract_id(CONTRACT_2_ID)
				.write();
	}

	private static void writeInst5Positions() {
		Position.defaultPosition().ric(INST_5_RIC).position_type("S").td_quantity(INST_5_S_TD_QTY)
				.swap_contract_id(67890).write();
		Position.defaultPosition().ric(INST_5_RIC).position_type("E").td_quantity(55).swap_contract_id(CONTRACT_2_ID)
				.write();
		Position.defaultPosition().ric(INST_5_RIC).position_type("I").td_quantity(45).swap_contract_id(CONTRACT_2_ID)
				.write();
	}

	private static void writeInst6Positions() {
		Position.defaultPosition().ric(INST_6_RIC).position_type("S").td_quantity(INST_6_S_TD_QTY)
				.swap_contract_id(67890).write();
		Position.defaultPosition().ric(INST_6_RIC).position_type("E").td_quantity(25).swap_contract_id(CONTRACT_2_ID)
				.write();
		Position.defaultPosition().ric(INST_6_RIC).position_type("I").td_quantity(15).swap_contract_id(CONTRACT_2_ID)
				.write();
	}

	private static void writeInst7Positions() {
		Position.defaultPosition().ric(INST_7_RIC).position_type("S").td_quantity(INST_7_S_TD_QTY)
				.swap_contract_id(54321).write();
		Position.defaultPosition().ric(INST_7_RIC).position_type("E").td_quantity(87).swap_contract_id(CONTRACT_3_ID)
				.write();
		Position.defaultPosition().ric(INST_7_RIC).position_type("I").td_quantity(77).swap_contract_id(CONTRACT_3_ID)
				.write();
	}

	private static void writeInst8Positions() {
		Position.defaultPosition().ric(INST_8_RIC).position_type("S").td_quantity(INST_8_S_TD_QTY)
				.swap_contract_id(54321).write();
		Position.defaultPosition().ric(INST_8_RIC).position_type("E").td_quantity(57).swap_contract_id(CONTRACT_3_ID)
				.write();
		Position.defaultPosition().ric(INST_8_RIC).position_type("I").td_quantity(47).swap_contract_id(CONTRACT_3_ID)
				.write();
	}

	private static void writeInst9Positions() {
		Position.defaultPosition().ric(INST_9_RIC).position_type("S").td_quantity(INST_9_S_TD_QTY)
				.swap_contract_id(54321).write();
		Position.defaultPosition().ric(INST_9_RIC).position_type("E").td_quantity(27).swap_contract_id(CONTRACT_3_ID)
				.write();
		Position.defaultPosition().ric(INST_9_RIC).position_type("I").td_quantity(17).swap_contract_id(CONTRACT_3_ID)
				.write();
	}

}
