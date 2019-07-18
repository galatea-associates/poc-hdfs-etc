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

public class MultipleEffectiveDatePositionsTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;

	private static final int	CONTRACT_1_ID		= 12345;
	private static final int	CONTRACT_2_ID		= 67890;

	private static final int	EFFECTIVE_DATE_1	= 20181231;
	private static final int	EFFECTIVE_DATE_2	= 20190101;

	private static final String	INST_1_RIC			= "ABC";
	private static final int	INST_1_ID			= 11;
	private static final int	INST_1_ED1_S_TD_QTY	= 90;
	private static final int	INST_1_ED2_S_TD_QTY	= 60;

	private static final String	INST_2_RIC			= "DEF";
	private static final int	INST_2_ID			= 22;
	private static final int	INST_2_ED1_S_TD_QTY	= 95;
	private static final int	INST_2_ED2_S_TD_QTY	= 55;

	private static final String	INST_3_RIC			= "GHI";
	private static final int	INST_3_ID			= 33;
	private static final int	INST_3_ED1_S_TD_QTY	= 97;
	private static final int	INST_3_ED2_S_TD_QTY	= 67;

	@BeforeClass
	public static void writeData() {
		writeContracts();
		CounterParty.defaultCounterParty().write();
		writeInstruments();
		writePositions();
	}

	private static void writeContracts() {
		Contract.defaultContract().swap_contract_id(CONTRACT_1_ID).write();
		Contract.defaultContract().swap_contract_id(CONTRACT_2_ID).write();
	}

	private static void writeInstruments() {
		Instrument.defaultInstrument().ric(INST_1_RIC).instrument_id(INST_1_ID).write();
		Instrument.defaultInstrument().ric(INST_2_RIC).instrument_id(INST_2_ID).write();
		Instrument.defaultInstrument().ric(INST_3_RIC).instrument_id(INST_3_ID).write();
	}

	private static void writePositions() {
		writeInst1Positions();
		writeInst2Positions();
		writeInst3Positions();
	}

	@Test
	public void testEffectiveDate1Query() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK, EFFECTIVE_DATE_1);

		results.assertResultCountEquals(3);

		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_1_RIC).effectiveDate(EFFECTIVE_DATE_1)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_1_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_1_ED1_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_2_RIC).effectiveDate(EFFECTIVE_DATE_1)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_2_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_2_ED1_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_3_RIC).effectiveDate(EFFECTIVE_DATE_1)
				.swapId(CONTRACT_2_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_3_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_3_ED1_S_TD_QTY).book(Defaults.BOOK));
	}

	@Test
	public void testEffectiveDate2Query() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK, EFFECTIVE_DATE_2);

		results.assertResultCountEquals(3);

		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_1_RIC).effectiveDate(EFFECTIVE_DATE_2)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_1_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_1_ED2_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_2_RIC).effectiveDate(EFFECTIVE_DATE_2)
				.swapId(CONTRACT_1_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_2_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_2_ED2_S_TD_QTY).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric(INST_3_RIC).effectiveDate(EFFECTIVE_DATE_2)
				.swapId(CONTRACT_2_ID).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(INST_3_ID)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(INST_3_ED2_S_TD_QTY).book(Defaults.BOOK));
	}

	@Test
	public void testNoEffectiveDatePositions() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE + 1);
		results.assertResultCountEquals(0);
	}

	@Test
	public void testNoCounterParty() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults("missingBook", 20190101);
		results.assertResultCountEquals(0);
	}

	private static void writeInst1Positions() {
		Position.defaultPosition().ric(INST_1_RIC).position_type("S").td_quantity(INST_1_ED1_S_TD_QTY)
				.swap_contract_id(CONTRACT_1_ID).effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("E").td_quantity(80).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("I").td_quantity(70).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("S").td_quantity(INST_1_ED2_S_TD_QTY)
				.swap_contract_id(CONTRACT_1_ID).effective_date(EFFECTIVE_DATE_2).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("E").td_quantity(50).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_2).write();
		Position.defaultPosition().ric(INST_1_RIC).position_type("I").td_quantity(40).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_2).write();
	}

	private static void writeInst2Positions() {
		Position.defaultPosition().ric(INST_2_RIC).position_type("S").td_quantity(INST_2_ED1_S_TD_QTY)
				.swap_contract_id(CONTRACT_1_ID).effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("E").td_quantity(85).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("I").td_quantity(75).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("S").td_quantity(INST_2_ED2_S_TD_QTY)
				.swap_contract_id(CONTRACT_1_ID).effective_date(EFFECTIVE_DATE_2).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("E").td_quantity(55).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_2).write();
		Position.defaultPosition().ric(INST_2_RIC).position_type("I").td_quantity(45).swap_contract_id(CONTRACT_1_ID)
				.effective_date(EFFECTIVE_DATE_2).write();
	}

	private static void writeInst3Positions() {
		Position.defaultPosition().ric(INST_3_RIC).position_type("S").td_quantity(INST_3_ED1_S_TD_QTY)
				.swap_contract_id(CONTRACT_2_ID).effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("E").td_quantity(87).swap_contract_id(CONTRACT_2_ID)
				.effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("I").td_quantity(77).swap_contract_id(CONTRACT_2_ID)
				.effective_date(EFFECTIVE_DATE_1).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("S").td_quantity(INST_3_ED2_S_TD_QTY)
				.swap_contract_id(CONTRACT_2_ID).effective_date(EFFECTIVE_DATE_2).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("E").td_quantity(57).swap_contract_id(CONTRACT_2_ID)
				.effective_date(EFFECTIVE_DATE_2).write();
		Position.defaultPosition().ric(INST_3_RIC).position_type("I").td_quantity(47).swap_contract_id(CONTRACT_2_ID)
				.effective_date(EFFECTIVE_DATE_2).write();
	}

}
