package org.galatea.pochdfs.test.unpaidcash;

import org.galatea.pochdfs.data.MegaTestData;
import org.galatea.pochdfs.domain.result.UnpaidCash;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class MegaTest extends SwapQueryTest {

	private static final long	serialVersionUID							= 1L;

	private static final String	BOOK_1										= MegaTestData.BOOK_1;

	// private static final int BOOK_1_EFFECTIVE_DATE_1 =
	// MegaTestData.BOOK_1_CASHFLOW_DIV_PD - 1;
	private static final String	BOOK_1_EFFECTIVE_DATE_2						= MegaTestData.BOOK_1_CASHFLOW_INT_PD_1_ED_1;
	private static final String	BOOK_1_EFFECTIVE_DATE_3						= MegaTestData.BOOK_1_CASHFLOW_INT_PD_1_ED_2;
	private static final String	BOOK_1_EFFECTIVE_DATE_4						= MegaTestData.BOOK_1_CASHFLOW_INT_PD_2_ED_1;
	private static final String	BOOK_1_EFFECTIVE_DATE_5						= MegaTestData.BOOK_1_CASHFLOW_INT_PD_2_ED_2;

	private static final double	BOOK_1_SWAP_1_POS_1_UNPAID_CASH_DIV			= MegaTestData.BOOK_1_SWAP_1_POS_1_CASHFLOW_DIV_PD_ED_AMT;
	private static final double	BOOK_1_SWAP_1_POS_2_UNPAID_CASH_DIV			= MegaTestData.BOOK_1_SWAP_1_POS_2_CASHFLOW_DIV_PD_ED_AMT;
	private static final double	BOOK_1_SWAP_1_POS_3_UNPAID_CASH_DIV			= MegaTestData.BOOK_1_SWAP_1_POS_3_CASHFLOW_DIV_PD_ED_AMT;
	private static final double	BOOK_1_SWAP_2_POS_1_UNPAID_CASH_DIV			= MegaTestData.BOOK_1_SWAP_2_POS_1_CASHFLOW_DIV_PD_ED_AMT;
	private static final double	BOOK_1_SWAP_2_POS_2_UNPAID_CASH_DIV			= MegaTestData.BOOK_1_SWAP_2_POS_2_CASHFLOW_DIV_PD_ED_AMT;

	private static final double	BOOK_1_SWAP_1_POS_1_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT;
	private static final double	BOOK_1_SWAP_1_POS_2_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT;
	private static final double	BOOK_1_SWAP_1_POS_3_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_1_ED_1_AMT;
	private static final double	BOOK_1_SWAP_2_POS_1_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT;
	private static final double	BOOK_1_SWAP_2_POS_2_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT;

	private static final double	BOOK_1_SWAP_1_POS_1_ED_3_UNPAID_CASH_INT	= BOOK_1_SWAP_1_POS_1_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT;
	private static final double	BOOK_1_SWAP_1_POS_2_ED_3_UNPAID_CASH_INT	= BOOK_1_SWAP_1_POS_2_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT;
	private static final double	BOOK_1_SWAP_1_POS_3_ED_3_UNPAID_CASH_INT	= BOOK_1_SWAP_1_POS_3_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_1_ED_2_AMT;
	private static final double	BOOK_1_SWAP_2_POS_1_ED_3_UNPAID_CASH_INT	= BOOK_1_SWAP_2_POS_1_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT;
	private static final double	BOOK_1_SWAP_2_POS_2_ED_3_UNPAID_CASH_INT	= BOOK_1_SWAP_2_POS_2_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT;

	private static final double	BOOK_1_SWAP_1_POS_1_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT;
	private static final double	BOOK_1_SWAP_1_POS_2_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT;
	private static final double	BOOK_1_SWAP_1_POS_3_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_2_ED_1_AMT;
	private static final double	BOOK_1_SWAP_2_POS_1_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT;
	private static final double	BOOK_1_SWAP_2_POS_2_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT;

	private static final double	BOOK_1_SWAP_1_POS_1_ED_5_UNPAID_CASH_INT	= BOOK_1_SWAP_1_POS_1_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT;
	private static final double	BOOK_1_SWAP_1_POS_2_ED_5_UNPAID_CASH_INT	= BOOK_1_SWAP_1_POS_2_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT;
	private static final double	BOOK_1_SWAP_1_POS_3_ED_5_UNPAID_CASH_INT	= BOOK_1_SWAP_1_POS_3_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_2_ED_2_AMT;
	private static final double	BOOK_1_SWAP_2_POS_1_ED_5_UNPAID_CASH_INT	= BOOK_1_SWAP_2_POS_1_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT;
	private static final double	BOOK_1_SWAP_2_POS_2_ED_5_UNPAID_CASH_INT	= BOOK_1_SWAP_2_POS_2_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT;

	private static final String	BOOK_2										= MegaTestData.BOOK_2;

	// private static final int BOOK_2_EFFECTIVE_DATE_1 =
	// MegaTestData.BOOK_2_CASHFLOW_DIV_PD - 1;
	private static final String	BOOK_2_EFFECTIVE_DATE_2						= MegaTestData.BOOK_2_CASHFLOW_INT_PD_1_ED_1;
	private static final String	BOOK_2_EFFECTIVE_DATE_3						= MegaTestData.BOOK_2_CASHFLOW_INT_PD_1_ED_2;
	private static final String	BOOK_2_EFFECTIVE_DATE_4						= MegaTestData.BOOK_2_CASHFLOW_INT_PD_2_ED_1;
	private static final String	BOOK_2_EFFECTIVE_DATE_5						= MegaTestData.BOOK_2_CASHFLOW_INT_PD_2_ED_2;

	private static final double	BOOK_2_SWAP_1_POS_1_UNPAID_CASH_DIV			= MegaTestData.BOOK_2_SWAP_1_POS_1_CASHFLOW_DIV_PD_ED_AMT;
	private static final double	BOOK_2_SWAP_2_POS_1_UNPAID_CASH_DIV			= MegaTestData.BOOK_2_SWAP_2_POS_1_CASHFLOW_DIV_PD_ED_AMT;
	private static final double	BOOK_2_SWAP_2_POS_2_UNPAID_CASH_DIV			= MegaTestData.BOOK_2_SWAP_2_POS_2_CASHFLOW_DIV_PD_ED_AMT;

	private static final double	BOOK_2_SWAP_1_POS_1_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT;
	private static final double	BOOK_2_SWAP_2_POS_1_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT;
	private static final double	BOOK_2_SWAP_2_POS_2_ED_2_UNPAID_CASH_INT	= MegaTestData.BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT;

	private static final double	BOOK_2_SWAP_1_POS_1_ED_3_UNPAID_CASH_INT	= BOOK_2_SWAP_1_POS_1_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT;
	private static final double	BOOK_2_SWAP_2_POS_1_ED_3_UNPAID_CASH_INT	= BOOK_2_SWAP_2_POS_1_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT;
	private static final double	BOOK_2_SWAP_2_POS_2_ED_3_UNPAID_CASH_INT	= BOOK_2_SWAP_2_POS_2_ED_2_UNPAID_CASH_INT
			+ MegaTestData.BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT;

	private static final double	BOOK_2_SWAP_1_POS_1_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT;
	private static final double	BOOK_2_SWAP_2_POS_1_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT;
	private static final double	BOOK_2_SWAP_2_POS_2_ED_4_UNPAID_CASH_INT	= MegaTestData.BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT;

	private static final double	BOOK_2_SWAP_1_POS_1_ED_5_UNPAID_CASH_INT	= BOOK_2_SWAP_1_POS_1_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT;
	private static final double	BOOK_2_SWAP_2_POS_1_ED_5_UNPAID_CASH_INT	= BOOK_2_SWAP_2_POS_1_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT;
	private static final double	BOOK_2_SWAP_2_POS_2_ED_5_UNPAID_CASH_INT	= BOOK_2_SWAP_2_POS_2_ED_4_UNPAID_CASH_INT
			+ MegaTestData.BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT;

	@BeforeClass
	public static void writeData() {
		MegaTestData.writeCounterParties();
		MegaTestData.writeSwapContracts();
		MegaTestData.writeCashFlows();
	}

	@Test
	public void testBook1EffectiveDate2UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_1, BOOK_1_EFFECTIVE_DATE_2);

		results.assertResultCountEquals(5);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_1_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_2_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_3_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_3_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_3_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_1_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_2_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));
	}

	@Test
	public void testBook1EffectiveDate3UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_1, BOOK_1_EFFECTIVE_DATE_3);

		results.assertResultCountEquals(5);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_1_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_2_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_3_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_3_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_3_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_1_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_2_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));
	}

	@Test
	public void testBook1EffectiveDate4UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_1, BOOK_1_EFFECTIVE_DATE_4);

		results.assertResultCountEquals(5);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_1_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_2_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_3_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_3_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_3_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_1_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_2_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));
	}

	@Test
	public void testBook1EffectiveDate5UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_1, BOOK_1_EFFECTIVE_DATE_5);

		results.assertResultCountEquals(5);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_1_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_2_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_1_POS_3_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_1_POS_3_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_1_POSTITION_3_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_1_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_1_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_1_SWAP_2_POS_2_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_1_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_1_SWAP_2_ID));
	}

	@Test
	public void testBook2EffectiveDate2UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_2, BOOK_2_EFFECTIVE_DATE_2);

		results.assertResultCountEquals(3);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_1_POS_1_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_1_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_2_ED_2_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));
	}

	@Test
	public void testBook2EffectiveDate3UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_2, BOOK_2_EFFECTIVE_DATE_3);

		results.assertResultCountEquals(3);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_1_POS_1_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_1_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_2_ED_3_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));
	}

	@Test
	public void testBook2EffectiveDate4UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_2, BOOK_2_EFFECTIVE_DATE_4);

		results.assertResultCountEquals(3);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_1_POS_1_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_1_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_2_ED_4_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));
	}

	@Test
	public void testBook2EffectiveDate5UnpaidCashQuery() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_2, BOOK_2_EFFECTIVE_DATE_5);

		results.assertResultCountEquals(3);

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_1_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_1_POS_1_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_1_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_1_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_1_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_1_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_1_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));

		results.assertHasUnpaidCash(new UnpaidCash().unpaidDiv(BOOK_2_SWAP_2_POS_2_UNPAID_CASH_DIV)
				.unpaidInt(BOOK_2_SWAP_2_POS_2_ED_5_UNPAID_CASH_INT).ric(MegaTestData.BOOK_2_SWAP_2_POSTITION_2_RIC)
				.swapId(MegaTestData.BOOK_2_SWAP_2_ID));
	}

	@Test
	public void testBook1NoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_1, "2019-12-12");
		results.assertResultCountEquals(0);
	}

	@Test
	public void testBook2NoEffectiveDateUnpaidCash() {
		UnpaidCashResults results = resultGetter.getUnpaidCashResults(BOOK_2, "2019-12-12");
		results.assertResultCountEquals(0);
	}

}
