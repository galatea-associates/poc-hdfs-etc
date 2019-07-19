package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.result.UnpaidCash;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultipleInstrumentsTest extends SwapQueryTest {

	private static final long	serialVersionUID		= 1L;

	private static final String	RIC_1					= "ABC";
	private static final String	RIC_2					= "DEF";
	private static final String	RIC_3					= "GHI";

	private static final double	INSTRUMENT_1_DIV_AMT	= 10;
	private static final double	INSTRUMENT_1_INT_AMT	= 20;
	private static final double	INSTRUMENT_2_DIV_AMT	= 30;
	private static final double	INSTRUMENT_2_INT_AMT	= 40;
	private static final double	INSTRUMENT_3_DIV_AMT	= 50;
	private static final double	INSTRUMENT_3_INT_AMT	= 60;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		CashFlow.defaultCashFlow().amount(INSTRUMENT_1_DIV_AMT).cashflow_type("DIV").ric(RIC_1).write();
		CashFlow.defaultCashFlow().amount(INSTRUMENT_1_INT_AMT).cashflow_type("INT").ric(RIC_1).write();
		CashFlow.defaultCashFlow().amount(INSTRUMENT_2_DIV_AMT).cashflow_type("DIV").ric(RIC_2).write();
		CashFlow.defaultCashFlow().amount(INSTRUMENT_2_INT_AMT).cashflow_type("INT").ric(RIC_2).write();
		CashFlow.defaultCashFlow().amount(INSTRUMENT_3_DIV_AMT).cashflow_type("DIV").ric(RIC_3).write();
		CashFlow.defaultCashFlow().amount(INSTRUMENT_3_INT_AMT).cashflow_type("INT").ric(RIC_3).write();
	}

	@Test
	public void testUnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		results.assertResultCountEquals(3);

		results.assertHasUnpaidCash(new UnpaidCash().ric(RIC_1).swapId(Defaults.CONTRACT_ID)
				.unpaidDiv(INSTRUMENT_1_DIV_AMT).unpaidInt(INSTRUMENT_1_INT_AMT));
		results.assertHasUnpaidCash(new UnpaidCash().ric(RIC_2).swapId(Defaults.CONTRACT_ID)
				.unpaidDiv(INSTRUMENT_2_DIV_AMT).unpaidInt(INSTRUMENT_2_INT_AMT));
		results.assertHasUnpaidCash(new UnpaidCash().ric(RIC_3).swapId(Defaults.CONTRACT_ID)
				.unpaidDiv(INSTRUMENT_3_DIV_AMT).unpaidInt(INSTRUMENT_3_INT_AMT));
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, "2019-12-12");
		results.assertResultCountEquals(0);
	}

}
