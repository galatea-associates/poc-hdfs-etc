package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class NoCounterPartyTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testUnpaidCashQuery() {
		new Contract().swap_contract_id(78901).write();
		new CashFlow().cashflow_id(1).swap_contract_id(78901).amount(100).cashflow_type("DIV").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(2).swap_contract_id(78901).amount(-50).cashflow_type("INT").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();

		UnpaidCashResults unpaidCashResults = resultGetter.getUnpaidCashResults(201, 20190101);

		unpaidCashResults.assertResultCountEquals(0);
	}

}
