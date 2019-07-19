package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.result.UnpaidCashResult;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;

	private static final double	DIV_AMT				= 100;
	private static final double	INT_AMT				= -50;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		CashFlow.defaultCashFlow().amount(DIV_AMT).cashflow_type("DIV").write();
		CashFlow.defaultCashFlow().amount(INT_AMT).cashflow_type("INT").write();
	}

	@Test
	public void testUnpaidCashQuery() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		result.assertRicEquals(Defaults.RIC);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(INT_AMT);
		result.assertUnpaidDivEquals(DIV_AMT);
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, "2019-01-02");
		results.assertResultCountEquals(0);
	}

}