package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.result.UnpaidCash;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.Test;

public class MultiplePaydatesTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testUnpaidCashQuery() {
		new Contract().counterparty_id(200).swap_contract_id(12345).write();

		new CashFlow().cashflow_id(1).swap_contract_id(12345).amount(10).cashflow_type("DIV").effective_date(20181220)
				.pay_date(20181226).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(2).swap_contract_id(12345).amount(-10).cashflow_type("INT").effective_date(20181220)
				.pay_date(20181226).instrument_id(11).long_short("Long").currency("USD").write();

		new CashFlow().cashflow_id(3).swap_contract_id(12345).amount(100).cashflow_type("DIV").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(4).swap_contract_id(12345).amount(-50).cashflow_type("INT").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").currency("USD").write();

		new CashFlow().cashflow_id(5).swap_contract_id(12345).amount(25).cashflow_type("DIV").effective_date(20190102)
				.pay_date(20190109).instrument_id(11).long_short("Long").currency("USD").write();
		new CashFlow().cashflow_id(6).swap_contract_id(12345).amount(-75).cashflow_type("INT").effective_date(20190102)
				.pay_date(20190109).instrument_id(11).long_short("Long").currency("USD").write();

		UnpaidCashResults unpaidCashResults = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		unpaidCashResults.assertResultCountEquals(1);
		unpaidCashResults.assertHasCashflow(new UnpaidCash().instId(11).swapId(12345).unpaidDiv(100).unpaidInt(-50));
	}

}
