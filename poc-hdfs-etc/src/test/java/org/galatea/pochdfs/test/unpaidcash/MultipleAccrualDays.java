package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class MultipleAccrualDays extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testUnpaidCashQuery() {
		new Contract().counterparty_id(200).swap_contract_id(56789).write();
		UnpaidCashResults unpaidCashResults = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);
		unpaidCashResults.assertResultCountEquals(0);
	}

}
