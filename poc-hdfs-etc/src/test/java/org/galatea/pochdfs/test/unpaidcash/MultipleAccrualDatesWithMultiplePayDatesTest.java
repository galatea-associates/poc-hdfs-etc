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

public class MultipleAccrualDatesWithMultiplePayDatesTest extends SwapQueryTest {

	private static final long	serialVersionUID			= 1L;

	private static final int	PAY_DATE_1					= 20181231;
	private static final int	PAY_DATE_2					= 20190103;

	private static final int	PAY_DATE_1_EFFECTIVE_DATE_1	= 20181228;
	private static final int	PAY_DATE_1_EFFECTIVE_DATE_2	= 20181229;
	private static final int	PAY_DATE_1_EFFECTIVE_DATE_3	= 20181230;
	private static final int	PAY_DATE_2_EFFECTIVE_DATE_1	= 20181231;
	private static final int	PAY_DATE_2_EFFECTIVE_DATE_2	= 20190101;
	private static final int	PAY_DATE_2_EFFECTIVE_DATE_3	= 20190102;

	private static final double	PAY_DATE_1_AMT_DIV_1		= 10;
	private static final double	PAY_DATE_1_AMT_DIV_2		= 30;
	private static final double	PAY_DATE_1_AMT_DIV_3		= 50;
	private static final double	PAY_DATE_1_AMT_INT_1		= 20;
	private static final double	PAY_DATE_1_AMT_INT_2		= 40;
	private static final double	PAY_DATE_1_AMT_INT_3		= 60;
	private static final double	PAY_DATE_2_AMT_DIV_1		= -95;
	private static final double	PAY_DATE_2_AMT_DIV_2		= -75;
	private static final double	PAY_DATE_2_AMT_DIV_3		= -55;
	private static final double	PAY_DATE_2_AMT_INT_1		= -85;
	private static final double	PAY_DATE_2_AMT_INT_2		= -65;
	private static final double	PAY_DATE_2_AMT_INT_3		= -45;

	@BeforeClass
	public static void writeData() {
		CounterParty.defaultCounterParty().write();
		Contract.defaultContract().write();
		writeCashFlows();
	}

	private static void writeCashFlows() {
		writePayDate1CashFlows();
		writePayDate2CashFlows();
	}

	@Test
	public void testPayDate1EffectiveDate1UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, PAY_DATE_1_EFFECTIVE_DATE_1);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidDivEquals(PAY_DATE_1_AMT_DIV_1);
		result.assertUnpaidIntEquals(PAY_DATE_1_AMT_INT_1);
	}

	@Test
	public void testPayDate1EffectiveDate2UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, PAY_DATE_1_EFFECTIVE_DATE_2);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidDivEquals(PAY_DATE_1_AMT_DIV_1 + PAY_DATE_1_AMT_DIV_2);
		result.assertUnpaidIntEquals(PAY_DATE_1_AMT_INT_1 + PAY_DATE_1_AMT_INT_2);
	}

	@Test
	public void testPayDate1EffectiveDate3UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, PAY_DATE_1_EFFECTIVE_DATE_3);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidDivEquals(PAY_DATE_1_AMT_DIV_1 + PAY_DATE_1_AMT_DIV_2 + PAY_DATE_1_AMT_DIV_3);
		result.assertUnpaidIntEquals(PAY_DATE_1_AMT_INT_1 + PAY_DATE_1_AMT_INT_2 + PAY_DATE_1_AMT_INT_3);
	}

	@Test
	public void testPayDate2EffectiveDate1UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, PAY_DATE_2_EFFECTIVE_DATE_1);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidDivEquals(PAY_DATE_2_AMT_DIV_1);
		result.assertUnpaidIntEquals(PAY_DATE_2_AMT_INT_1);
	}

	@Test
	public void testPayDate2EffectiveDate2UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, PAY_DATE_2_EFFECTIVE_DATE_2);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidDivEquals(PAY_DATE_2_AMT_DIV_1 + PAY_DATE_2_AMT_DIV_2);
		result.assertUnpaidIntEquals(PAY_DATE_2_AMT_INT_1 + PAY_DATE_2_AMT_INT_2);
	}

	@Test
	public void testPayDate2EffectiveDate3UnpaidCash() {
		UnpaidCashResult result = resultGetter.getSingleUnpaidCashResult(Defaults.BOOK, PAY_DATE_2_EFFECTIVE_DATE_3);

		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertUnpaidDivEquals(PAY_DATE_2_AMT_DIV_1 + PAY_DATE_2_AMT_DIV_2 + PAY_DATE_2_AMT_DIV_3);
		result.assertUnpaidIntEquals(PAY_DATE_2_AMT_INT_1 + PAY_DATE_2_AMT_INT_2 + PAY_DATE_2_AMT_INT_3);
	}

	@Test
	public void testNoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(Defaults.BOOK, PAY_DATE_2_EFFECTIVE_DATE_3 + 1);
		results.assertResultCountEquals(0);
	}

	private static void writePayDate1CashFlows() {
		CashFlow.defaultCashFlow().amount(PAY_DATE_1_AMT_DIV_1).cashflow_type("DIV")
				.effective_date(PAY_DATE_1_EFFECTIVE_DATE_1).pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_1_AMT_INT_1).cashflow_type("INT")
				.effective_date(PAY_DATE_1_EFFECTIVE_DATE_1).pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_1_AMT_DIV_2).cashflow_type("DIV")
				.effective_date(PAY_DATE_1_EFFECTIVE_DATE_2).pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_1_AMT_INT_2).cashflow_type("INT")
				.effective_date(PAY_DATE_1_EFFECTIVE_DATE_2).pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_1_AMT_DIV_3).cashflow_type("DIV")
				.effective_date(PAY_DATE_1_EFFECTIVE_DATE_3).pay_date(PAY_DATE_1).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_1_AMT_INT_3).cashflow_type("INT")
				.effective_date(PAY_DATE_1_EFFECTIVE_DATE_3).pay_date(PAY_DATE_1).write();
	}

	private static void writePayDate2CashFlows() {
		CashFlow.defaultCashFlow().amount(PAY_DATE_2_AMT_DIV_1).cashflow_type("DIV")
				.effective_date(PAY_DATE_2_EFFECTIVE_DATE_1).pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_2_AMT_INT_1).cashflow_type("INT")
				.effective_date(PAY_DATE_2_EFFECTIVE_DATE_1).pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_2_AMT_DIV_2).cashflow_type("DIV")
				.effective_date(PAY_DATE_2_EFFECTIVE_DATE_2).pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_2_AMT_INT_2).cashflow_type("INT")
				.effective_date(PAY_DATE_2_EFFECTIVE_DATE_2).pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_2_AMT_DIV_3).cashflow_type("DIV")
				.effective_date(PAY_DATE_2_EFFECTIVE_DATE_3).pay_date(PAY_DATE_2).write();
		CashFlow.defaultCashFlow().amount(PAY_DATE_2_AMT_INT_3).cashflow_type("INT")
				.effective_date(PAY_DATE_2_EFFECTIVE_DATE_3).pay_date(PAY_DATE_2).write();
	}

}
