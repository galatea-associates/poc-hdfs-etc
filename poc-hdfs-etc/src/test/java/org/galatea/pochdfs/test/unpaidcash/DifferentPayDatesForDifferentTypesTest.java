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

public class DifferentPayDatesForDifferentTypesTest extends SwapQueryTest {

	private static final long	serialVersionUID		= 1L;

	private static final int	DIV_PAY_DATE			= 20190601;
	private static final int	INT_PAY_DATE			= 20190102;

	private static final int	DIV_EFFECTIVE_DATE_1	= 20181201;
	private static final int	INT_EFFECTIVE_DATE_1	= 20181230;
	private static final int	INT_EFFECTIVE_DATE_2	= 20181231;
	private static final int	INT_EFFECTIVE_DATE_3	= 20190101;

	private static final double	CASHFLOW_DIV_1_AMT		= 125;
	private static final double	CASHFLOW_INT_1_AMT		= 30;
	private static final double	CASHFLOW_INT_2_AMT		= 200;
	private static final double	CASHFLOW_INT_3_AMT		= -25;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_DIV_1_AMT).cashflow_type("DIV").effective_date(DIV_EFFECTIVE_DATE_1)
				.pay_date(DIV_PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_INT_1_AMT).cashflow_type("INT").effective_date(INT_EFFECTIVE_DATE_1)
				.pay_date(INT_PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_INT_2_AMT).cashflow_type("INT").effective_date(INT_EFFECTIVE_DATE_2)
				.pay_date(INT_PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_INT_3_AMT).cashflow_type("INT").effective_date(INT_EFFECTIVE_DATE_3)
				.pay_date(INT_PAY_DATE).write();
	}

	@Test
	public void testDivEffectiveDateUnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, DIV_EFFECTIVE_DATE_1);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidTypeDoesNotExist("INT");
		result.assertUnpaidDivEquals(CASHFLOW_DIV_1_AMT);
	}

	@Test
	public void testIntEffectiveDate1UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, INT_EFFECTIVE_DATE_1);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_INT_1_AMT);
		result.assertUnpaidDivEquals(CASHFLOW_DIV_1_AMT);
	}

	@Test
	public void testIntEffectiveDate2UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, INT_EFFECTIVE_DATE_2);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_INT_1_AMT + CASHFLOW_INT_2_AMT);
		result.assertUnpaidDivEquals(CASHFLOW_DIV_1_AMT);
	}

	@Test
	public void testIntEffectiveDate3UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, INT_EFFECTIVE_DATE_3);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_INT_1_AMT + CASHFLOW_INT_2_AMT + CASHFLOW_INT_3_AMT);
		result.assertUnpaidDivEquals(CASHFLOW_DIV_1_AMT);
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, DIV_PAY_DATE + 1);
		results.assertResultCountEquals(0);
	}

}
