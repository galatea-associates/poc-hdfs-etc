package org.galatea.test;

import org.galatea.domain.input.CashFlow;
import org.galatea.domain.input.Contract;
import org.galatea.domain.result.UnpaidCash;
import org.galatea.domain.result.UnpaidCashResults;
import org.galatea.util.SwapQueryTest;
import org.junit.Test;

public class SimpleUnpaidCashTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testUnpaidCashQuery() {
		new Contract().counterparty_id(200).swap_contract_id(12345).write();
		new CashFlow().cashflow_id(1).swap_contract_id(12345).amount(100).cashflow_type("DIV").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").write();
		new CashFlow().cashflow_id(2).swap_contract_id(12345).amount(-50).cashflow_type("INT").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").write();

		UnpaidCashResults unpaidCashResults = resultGetter.getUnpaidCashResults(200, 20190101);

		unpaidCashResults.assertResultCountEquals(1);
		unpaidCashResults.assertHasCashflow(new UnpaidCash().instId(11).swapId(12345).unpaidDiv(100).unpaidInt(-50));
	}

}
