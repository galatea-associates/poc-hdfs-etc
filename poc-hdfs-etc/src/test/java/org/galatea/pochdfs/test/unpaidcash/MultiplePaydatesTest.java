package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.result.UnpaidCashResult;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiplePaydatesTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;

	private static final int	PAY_DATE_1			= 20181226;
	private static final int	PAY_DATE_2			= 20190102;
	private static final int	PAY_DATE_3			= 20190109;

	private static final int	EFFECTIVE_DATE_1	= 20181220;
	private static final int	EFFECTIVE_DATE_2	= 20190101;
	private static final int	EFFECTIVE_DATE_3	= 20190102;

	private static final double	CASHFLOW_1_AMT_DIV	= 10;
	private static final double	CASHFLOW_1_AMT_INT	= -10;
	private static final double	CASHFLOW_2_AMT_DIV	= 100;
	private static final double	CASHFLOW_2_AMT_INT	= 50;
	private static final double	CASHFLOW_3_AMT_DIV	= 25;
	private static final double	CASHFLOW_3_AMT_INT	= -75;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_1_AMT_DIV).cashflow_type("DIV").effective_date(EFFECTIVE_DATE_1)
				.pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_1_AMT_INT).cashflow_type("INT").effective_date(EFFECTIVE_DATE_1)
				.pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_2_AMT_DIV).cashflow_type("DIV").effective_date(EFFECTIVE_DATE_2)
				.pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_2_AMT_INT).cashflow_type("INT").effective_date(EFFECTIVE_DATE_2)
				.pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_3_AMT_DIV).cashflow_type("DIV").effective_date(EFFECTIVE_DATE_3)
				.pay_date(PAY_DATE_3).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_3_AMT_INT).cashflow_type("INT").effective_date(EFFECTIVE_DATE_3)
				.pay_date(PAY_DATE_3).write();
	}

	@Test
	public void testEffectiveDate1UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, EFFECTIVE_DATE_1);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_1_AMT_INT);
		result.assertUnpaidDivEquals(CASHFLOW_1_AMT_DIV);
	}

	@Test
	public void testEffectiveDate2UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, EFFECTIVE_DATE_2);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_2_AMT_INT);
		result.assertUnpaidDivEquals(CASHFLOW_2_AMT_DIV);
	}

	@Test
	public void testEffectiveDate3UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, EFFECTIVE_DATE_3);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_3_AMT_INT);
		result.assertUnpaidDivEquals(CASHFLOW_3_AMT_DIV);
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, PAY_DATE_3 + 1);
		results.assertResultCountEquals(0);
	}

}