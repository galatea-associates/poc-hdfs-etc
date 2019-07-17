package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.result.UnpaidCashResult;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		CashFlow.defaultCashFlow().amount(100).cashflow_type("DIV").write();
		CashFlow.defaultCashFlow().amount(-50).cashflow_type("INT").write();
	}

	@Test
	public void testUnpaidCashQuery() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(-50.0);
		result.assertUnpaidDivEquals(100.0);
	}

}