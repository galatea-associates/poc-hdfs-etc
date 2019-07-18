package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class NoCashFlowsForContractTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
	}

	@Test
	public void testUnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);
		results.assertResultCountEquals(0);
	}

}
