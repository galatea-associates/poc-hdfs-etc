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

public class MultipleAccrualDatesTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;

	private static final String	PAY_DATE			= "2019-01-02";

	private static final String	EFFECTIVE_DATE_1	= "2018-12-30";
	private static final String	EFFECTIVE_DATE_2	= "2018-12-31";
	private static final String	EFFECTIVE_DATE_3	= "2019-01-01";

	private static final double	CASHFLOW_1_AMT_DIV	= 25;
	private static final double	CASHFLOW_1_AMT_INT	= -10;
	private static final double	CASHFLOW_2_AMT_DIV	= 100;
	private static final double	CASHFLOW_2_AMT_INT	= 50;
	private static final double	CASHFLOW_3_AMT_DIV	= 300;
	private static final double	CASHFLOW_3_AMT_INT	= -25;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_1_AMT_DIV).cashflow_type("DIV").effective_date(EFFECTIVE_DATE_1)
				.pay_date(PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_1_AMT_INT).cashflow_type("INT").effective_date(EFFECTIVE_DATE_1)
				.pay_date(PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_2_AMT_DIV).cashflow_type("DIV").effective_date(EFFECTIVE_DATE_2)
				.pay_date(PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_2_AMT_INT).cashflow_type("INT").effective_date(EFFECTIVE_DATE_2)
				.pay_date(PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_3_AMT_DIV).cashflow_type("DIV").effective_date(EFFECTIVE_DATE_3)
				.pay_date(PAY_DATE).write();
		CashFlow.defaultCashFlow().amount(CASHFLOW_3_AMT_INT).cashflow_type("INT").effective_date(EFFECTIVE_DATE_3)
				.pay_date(PAY_DATE).write();
	}

	@Test
	public void testEffectiveDate1UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, EFFECTIVE_DATE_1);

		result.assertRicEquals(Defaults.RIC);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_1_AMT_INT);
		result.assertUnpaidDivEquals(CASHFLOW_1_AMT_DIV);
	}

	@Test
	public void testEffectiveDate2UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, EFFECTIVE_DATE_2);

		result.assertRicEquals(Defaults.RIC);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_1_AMT_INT + CASHFLOW_2_AMT_INT);
		result.assertUnpaidDivEquals(CASHFLOW_1_AMT_DIV + CASHFLOW_2_AMT_DIV);
	}

	@Test
	public void testEffectiveDate3UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, EFFECTIVE_DATE_3);

		result.assertRicEquals(Defaults.RIC);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidIntEquals(CASHFLOW_1_AMT_INT + CASHFLOW_2_AMT_INT + CASHFLOW_3_AMT_INT);
		result.assertUnpaidDivEquals(CASHFLOW_1_AMT_DIV + CASHFLOW_2_AMT_DIV + CASHFLOW_3_AMT_DIV);
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, "2019-12-12");
		results.assertResultCountEquals(0);
	}

}
