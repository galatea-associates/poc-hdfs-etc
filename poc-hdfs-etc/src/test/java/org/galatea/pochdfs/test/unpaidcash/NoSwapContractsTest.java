package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class NoSwapContractsTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testUnpaidCashQuery() {
		new Contract().counterparty_id(200).write();
		new CashFlow().cashflow_id(1).swap_contract_id(12345).amount(100).cashflow_type("DIV").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(2).swap_contract_id(12345).amount(-50).cashflow_type("INT").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();

		UnpaidCashResults unpaidCashResults = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		unpaidCashResults.assertResultCountEquals(0);
	}

}
