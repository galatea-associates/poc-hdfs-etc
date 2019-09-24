package org.galatea.pochdfs.test.epwithuc;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.input.Instrument;
import org.galatea.pochdfs.domain.input.Position;
import org.galatea.pochdfs.domain.result.EnrichedPositionsWithUnpaidCash;
import org.galatea.pochdfs.domain.result.EnrichedPositionsWithUnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;
	private static final int	S_TD_QUANTITY		= 100;
	private static final double	DIV_AMT				= 100;
	private static final double	INT_AMT				= -50;

	@BeforeClass
	public static void writeData() {
		Contract.defaultContract().write();
		CounterParty.defaultCounterParty().write();
		Instrument.defaultInstrument().write();
		CashFlow.defaultCashFlow().amount(DIV_AMT).cashflow_type("DIV").write();
		CashFlow.defaultCashFlow().amount(INT_AMT).cashflow_type("INT").write();
		Position.defaultPosition().position_type("S").td_quantity(S_TD_QUANTITY).write();
		Position.defaultPosition().position_type("E").td_quantity(50).write();
		Position.defaultPosition().position_type("I").td_quantity(75).write();
	}

	@Test
	public void testEnrichedPositionsWithUnpaidCashQuery() {
		EnrichedPositionsWithUnpaidCashResults results = resultGetter
				.getEnrichedPositionsWithUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		results.assertResultCountEquals(1);
		results.assertHasEnrichedPositionWithUnpaidCash(new EnrichedPositionsWithUnpaidCash()
				.counterPartyField1(Defaults.COUNTERPARTY_FIELD1).counterPartyId(Defaults.COUNTERPARTY_ID)
				.effectiveDate(Defaults.EFFECTIVE_DATE).instId(Defaults.INSTRUMENT_ID).ric(Defaults.RIC)
				.swapId(Defaults.CONTRACT_ID).unpaidDiv(DIV_AMT).unpaidInt(INT_AMT));
	}

	@Test
	public void testNoEffectiveDatePositions() {
		EnrichedPositionsWithUnpaidCashResults results = resultGetter
				.getEnrichedPositionsWithUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE + 1);
		results.assertResultCountEquals(0);
	}

	@Test
	public void testNoCounterParty() {
		EnrichedPositionsWithUnpaidCashResults results = resultGetter
				.getEnrichedPositionsWithUnpaidCashResults("missingBook", Defaults.EFFECTIVE_DATE);
		results.assertResultCountEquals(0);
	}

}