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

	private static final long serialVersionUID = 1L;

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
	}

	private static void writeInstruments() {
		Instrument.defaultInstrument().ric("ABC").instrument_id(11).write();
		Instrument.defaultInstrument().ric("DEF").instrument_id(22).write();
		Instrument.defaultInstrument().ric("GHI").instrument_id(33).write();
	}

	private static void writePositions() {
		Position.defaultPosition().ric("ABC").position_type("S").td_quantity(100).swap_contract_id(12345)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("ABC").position_type("E").td_quantity(90).swap_contract_id(12345)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("ABC").position_type("I").td_quantity(80).swap_contract_id(12345)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("ABC").position_type("S").td_quantity(70).swap_contract_id(12345)
				.effective_date(20190101).write();
		Position.defaultPosition().ric("ABC").position_type("E").td_quantity(60).swap_contract_id(12345)
				.effective_date(20190101).write();
		Position.defaultPosition().ric("ABC").position_type("I").td_quantity(50).swap_contract_id(12345)
				.effective_date(20190101).write();

		Position.defaultPosition().ric("DEF").position_type("S").td_quantity(95).swap_contract_id(12345)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("DEF").position_type("E").td_quantity(85).swap_contract_id(12345)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("DEF").position_type("I").td_quantity(75).swap_contract_id(12345)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("DEF").position_type("S").td_quantity(65).swap_contract_id(12345)
				.effective_date(20190101).write();
		Position.defaultPosition().ric("DEF").position_type("E").td_quantity(55).swap_contract_id(12345)
				.effective_date(20190101).write();
		Position.defaultPosition().ric("DEF").position_type("I").td_quantity(45).swap_contract_id(12345)
				.effective_date(20190101).write();

		Position.defaultPosition().ric("DEF").position_type("S").td_quantity(97).swap_contract_id(67890)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("DEF").position_type("E").td_quantity(87).swap_contract_id(67890)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("DEF").position_type("I").td_quantity(77).swap_contract_id(67890)
				.effective_date(20181231).write();
		Position.defaultPosition().ric("DEF").position_type("S").td_quantity(67).swap_contract_id(67890)
				.effective_date(20190101).write();
		Position.defaultPosition().ric("DEF").position_type("E").td_quantity(57).swap_contract_id(67890)
				.effective_date(20190101).write();
		Position.defaultPosition().ric("DEF").position_type("I").td_quantity(47).swap_contract_id(67890)
				.effective_date(20190101).write();
	}

	@Test
	public void testEnrichedPositionsQuery() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE);

		results.assertResultCountEquals(3);

		results.assertHasEnrichedPosition(new EnrichedPosition().ric("ABC").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(12345).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(11)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(100).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("DEF").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(12345).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(22)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(70).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("GHI").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(12345).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(33)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(40).book(Defaults.BOOK));

		results.assertHasEnrichedPosition(new EnrichedPosition().ric("JKL").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(67890).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(44)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(95).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("MNO").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(67890).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(55)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(65).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("PQR").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(67890).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(66)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(35).book(Defaults.BOOK));

		results.assertHasEnrichedPosition(new EnrichedPosition().ric("STU").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(54321).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(77)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(97).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("VWX").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(54321).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(88)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(67).book(Defaults.BOOK));
		results.assertHasEnrichedPosition(new EnrichedPosition().ric("YZA").effectiveDate(Defaults.EFFECTIVE_DATE)
				.swapId(54321).counterPartyField1(Defaults.COUNTERPARTY_FIELD1).instId(99)
				.counterPartyId(Defaults.COUNTERPARTY_ID).tdQuantity(37).book(Defaults.BOOK));
	}

	@Test
	public void testNoEffectiveDatePositions() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK, 20190102);
		results.assertResultCountEquals(0);
	}

	@Test
	public void testNoCounterParty() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults("missingBook", 20190101);
		results.assertResultCountEquals(0);
	}

}
