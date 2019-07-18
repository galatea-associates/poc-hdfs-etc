package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.result.UnpaidCash;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultipleContractsTest extends SwapQueryTest {

	private static final long	serialVersionUID			= 1L;

	private static final int	SWAP_1_ID					= 12345;
	private static final int	SWAP_2_ID					= 67890;

	private static final int	INSTRUMENT_1_ID				= 11;
	private static final int	INSTRUMENT_2_ID				= 22;

	private static final double	SWAP_1_INSTRUMENT_1_DIV_AMT	= 10;
	private static final double	SWAP_1_INSTRUMENT_1_INT_AMT	= 20;
	private static final double	SWAP_2_INSTRUMENT_1_DIV_AMT	= 30;
	private static final double	SWAP_2_INSTRUMENT_1_INT_AMT	= 40;
	private static final double	SWAP_2_INSTRUMENT_2_DIV_AMT	= 50;
	private static final double	SWAP_2_INSTRUMENT_2_INT_AMT	= 60;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().swap_contract_id(SWAP_1_ID).write();
		Contract.defaultContract().swap_contract_id(SWAP_2_ID).write();
		CashFlow.defaultCashFlow().amount(SWAP_1_INSTRUMENT_1_DIV_AMT).cashflow_type("DIV")
				.instrument_id(INSTRUMENT_1_ID).swap_contract_id(SWAP_1_ID).write();
		CashFlow.defaultCashFlow().amount(SWAP_1_INSTRUMENT_1_INT_AMT).cashflow_type("INT")
				.instrument_id(INSTRUMENT_1_ID).swap_contract_id(SWAP_1_ID).write();
		CashFlow.defaultCashFlow().amount(SWAP_2_INSTRUMENT_1_DIV_AMT).cashflow_type("DIV")
				.instrument_id(INSTRUMENT_1_ID).swap_contract_id(SWAP_2_ID).write();
		CashFlow.defaultCashFlow().amount(SWAP_2_INSTRUMENT_1_INT_AMT).cashflow_type("INT")
				.instrument_id(INSTRUMENT_1_ID).swap_contract_id(SWAP_2_ID).write();
		CashFlow.defaultCashFlow().amount(SWAP_2_INSTRUMENT_2_DIV_AMT).cashflow_type("DIV")
				.instrument_id(INSTRUMENT_2_ID).swap_contract_id(SWAP_2_ID).write();
		CashFlow.defaultCashFlow().amount(SWAP_2_INSTRUMENT_2_INT_AMT).cashflow_type("INT")
				.instrument_id(INSTRUMENT_2_ID).swap_contract_id(SWAP_2_ID).write();
	}

	@Test
	public void testUnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE);

		results.assertResultCountEquals(3);

		results.assertHasCashflow(new UnpaidCash().instId(INSTRUMENT_1_ID).swapId(SWAP_1_ID)
				.unpaidDiv(SWAP_1_INSTRUMENT_1_DIV_AMT).unpaidInt(SWAP_1_INSTRUMENT_1_INT_AMT));
		results.assertHasCashflow(new UnpaidCash().instId(INSTRUMENT_1_ID).swapId(SWAP_2_ID)
				.unpaidDiv(SWAP_2_INSTRUMENT_1_DIV_AMT).unpaidInt(SWAP_2_INSTRUMENT_1_INT_AMT));
		results.assertHasCashflow(new UnpaidCash().instId(INSTRUMENT_2_ID).swapId(SWAP_2_ID)
				.unpaidDiv(SWAP_2_INSTRUMENT_2_DIV_AMT).unpaidInt(SWAP_2_INSTRUMENT_2_INT_AMT));
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, Defaults.EFFECTIVE_DATE + 1);
		results.assertResultCountEquals(0);
	}

}
