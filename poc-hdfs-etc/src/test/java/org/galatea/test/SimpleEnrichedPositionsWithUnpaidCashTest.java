package org.galatea.test;

import org.galatea.domain.input.CashFlow;
import org.galatea.domain.input.Contract;
import org.galatea.domain.input.CounterParty;
import org.galatea.domain.input.Instrument;
import org.galatea.domain.input.Position;
import org.galatea.domain.result.EnrichedPositionsWithUnpaidCash;
import org.galatea.domain.result.EnrichedPositionsWithUnpaidCashResults;
import org.galatea.util.SwapQueryTest;
import org.junit.Test;

public class SimpleEnrichedPositionsWithUnpaidCashTest extends SwapQueryTest {

	private static final long serialVersionUID = 1L;

	@Test
	public void testEnrichedPositionsWithUnpaidCashQuery() {
		new Contract().counterparty_id(200).swap_contract_id(12345).write();
		new CounterParty().counterparty_id(200).counterparty_field1("cp200Field1").write();
		new Instrument().instrument_id(11).ric("ABC").write();
		new Position().position_type("S").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("I").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new Position().position_type("E").swap_contract_id(12345).ric("ABC").effective_date(20190101).write();
		new CashFlow().cashflow_id(1).swap_contract_id(12345).amount(100).cashflow_type("DIV").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").write();
		new CashFlow().cashflow_id(2).swap_contract_id(12345).amount(-50).cashflow_type("INT").effective_date(20190101)
				.pay_date(20190102).instrument_id(11).long_short("Long").write();

		EnrichedPositionsWithUnpaidCashResults results = resultGetter.getEnrichedPositionsWithUnpaidCashResults(200,
				20190101);

		results.assertResultCountEquals(1);
		results.assertHasEnrichedPositionWithUnpaidCash(new EnrichedPositionsWithUnpaidCash().ric("ABC")
				.effectiveDate(20190101).swapId(12345).counterPartyField1("cp200Field1").instId(11).counterPartyId(200)
				.unpaidDiv(100).unpaidInt(-50));

	}

}
