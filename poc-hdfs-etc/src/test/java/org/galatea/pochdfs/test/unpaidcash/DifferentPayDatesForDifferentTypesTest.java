package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.result.UnpaidCash;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class DifferentPayDatesForDifferentTypesTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testUnpaidCashQuery() {
		new Contract().counterparty_id(200).swap_contract_id(12345).write();

		new CashFlow().cashflow_id(1).swap_contract_id(12345).amount(125).cashflow_type("DIV").effective_date(20181231)
				.pay_date(20190601).instrument_id(11).long_short("Long").currency("USD").write();

		new CashFlow().cashflow_id(2).swap_contract_id(12345).amount(30).cashflow_type("INT").effective_date(20181230)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(3).swap_contract_id(12345).amount(200).cashflow_type("INT").effective_date(20181231)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(4).swap_contract_id(12345).amount(-25).cashflow_type("INT").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();

		UnpaidCashResults unpaidCashResults = resultGetter.getUnpaidCashResults(200, 20190101);

		unpaidCashResults.assertResultCountEquals(1);
		unpaidCashResults.assertHasCashflow(new UnpaidCash().instId(11).swapId(12345).unpaidDiv(125).unpaidInt(205));
	}

}
