package org.galatea.pochdfs.util;

public class MegaTestData {

	public static final String	BOOK_1										= "AABBCC";
	public static final String	BOOK_2										= "DDEEFF";

	public static final int		BOOK_1_CP_ID								= 200;
	public static final int		BOOK_2_CP_ID								= 201;

	public static final String	BOOK_1_CP_FIELD								= "BOOK1";
	public static final String	BOOK_2_CP_FIELD								= "BOOK2";

	public static final int		BOOK_1_SWAP_1_ID							= 101;
	public static final int		BOOK_1_SWAP_2_ID							= 102;
	public static final int		BOOK_2_SWAP_1_ID							= 201;
	public static final int		BOOK_2_SWAP_2_ID							= 202;

	public static final String	INSTRUMENT_1_RIC							= "ABC";
	public static final String	INSTRUMENT_2_RIC							= "DEF";
	public static final String	INSTRUMENT_3_RIC							= "GHI";
	public static final String	INSTRUMENT_4_RIC							= "JKL";
	public static final String	INSTRUMENT_5_RIC							= "MNO";
	public static final String	INSTRUMENT_6_RIC							= "PQR";

	public static final int		INSTRUMENT_1_ID								= 11;
	public static final int		INSTRUMENT_2_ID								= 22;
	public static final int		INSTRUMENT_3_ID								= 33;
	public static final int		INSTRUMENT_4_ID								= 44;
	public static final int		INSTRUMENT_5_ID								= 55;
	public static final int		INSTRUMENT_6_ID								= 66;

	/*
	 * Positions will have effective dates for the same days cash flows accrue
	 */
	public static final String	BOOK_1_SWAP_1_POSTITION_1_RIC				= INSTRUMENT_1_RIC;
	public static final String	BOOK_1_SWAP_1_POSTITION_2_RIC				= INSTRUMENT_2_RIC;
	public static final String	BOOK_1_SWAP_1_POSTITION_3_RIC				= INSTRUMENT_3_RIC;
	public static final String	BOOK_1_SWAP_2_POSTITION_1_RIC				= INSTRUMENT_1_RIC;
	public static final String	BOOK_1_SWAP_2_POSTITION_2_RIC				= INSTRUMENT_4_RIC;
	public static final String	BOOK_2_SWAP_1_POSTITION_1_RIC				= INSTRUMENT_5_RIC;
	public static final String	BOOK_2_SWAP_2_POSTITION_2_RIC				= INSTRUMENT_5_RIC;
	public static final String	BOOK_2_SWAP_2_POSTITION_1_RIC				= INSTRUMENT_1_RIC;

	public static final int		BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_S		= 111;
	public static final int		BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_E		= 222;
	public static final int		BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_I		= 333;

	public static final int		BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_S		= 444;
	public static final int		BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_E		= 555;
	public static final int		BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_I		= 666;

	public static final int		BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_S		= 777;
	public static final int		BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_E		= 888;
	public static final int		BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_I		= 999;

	public static final int		BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_S		= 987;
	public static final int		BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_E		= 654;
	public static final int		BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_I		= 321;

	public static final int		BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_S		= 109;
	public static final int		BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_E		= 876;
	public static final int		BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_I		= 543;

	public static final int		BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_S		= 210;
	public static final int		BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_E		= 234;
	public static final int		BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_I		= 567;

	public static final int		BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_S		= 890;
	public static final int		BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_E		= 123;
	public static final int		BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_I		= 456;

	public static final int		BOOK_1_SWAP_1_CASHFLOW_DIV_PD				= 20190106;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_1				= 20190103;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_2				= 20190105;

	public static final int		BOOK_2_SWAP_1_CASHFLOW_DIV_PD				= 20190106;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_1				= 20190102;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_2				= 20190104;

	public static final int		BOOK_1_SWAP_1_CASHFLOW_DIV_ED				= BOOK_1_SWAP_1_CASHFLOW_DIV_PD - 5;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_1_ED_1		= BOOK_1_SWAP_1_CASHFLOW_INT_PD_1 - 2;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_1_ED_2		= BOOK_1_SWAP_1_CASHFLOW_INT_PD_1 - 1;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_2_ED_1		= BOOK_1_SWAP_1_CASHFLOW_INT_PD_2 - 2;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_2_ED_2		= BOOK_1_SWAP_1_CASHFLOW_INT_PD_2 - 1;

	public static final int		BOOK_2_SWAP_1_CASHFLOW_DIV_ED				= BOOK_2_SWAP_1_CASHFLOW_DIV_PD - 4;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_1_ED_1		= BOOK_2_SWAP_1_CASHFLOW_INT_PD_1 - 2;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_1_ED_2		= BOOK_2_SWAP_1_CASHFLOW_INT_PD_1 - 1;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_2_ED_1		= BOOK_2_SWAP_1_CASHFLOW_INT_PD_2 - 2;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_2_ED_2		= BOOK_2_SWAP_1_CASHFLOW_INT_PD_2 - 1;

	public static final int		BOOK_1_SWAP_2_CASHFLOW_DIV_PD				= 20190110;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_1				= 20190104;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_2				= 20190106;

	public static final int		BOOK_2_SWAP_2_CASHFLOW_DIV_PD				= 20190110;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_1				= 20190103;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_2				= 20190105;

	public static final int		BOOK_1_SWAP_2_CASHFLOW_DIV_ED				= BOOK_1_SWAP_2_CASHFLOW_DIV_PD - 5;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_1_ED_1		= BOOK_1_SWAP_2_CASHFLOW_INT_PD_1 - 2;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_1_ED_2		= BOOK_1_SWAP_2_CASHFLOW_INT_PD_1 - 1;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_2_ED_1		= BOOK_1_SWAP_2_CASHFLOW_INT_PD_2 - 2;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_2_ED_2		= BOOK_1_SWAP_2_CASHFLOW_INT_PD_2 - 1;

	public static final int		BOOK_2_SWAP_2_CASHFLOW_DIV_ED				= BOOK_2_SWAP_2_CASHFLOW_DIV_PD - 4;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_1_ED_1		= BOOK_2_SWAP_2_CASHFLOW_INT_PD_1 - 2;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_1_ED_2		= BOOK_2_SWAP_2_CASHFLOW_INT_PD_1 - 1;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_2_ED_1		= BOOK_2_SWAP_2_CASHFLOW_INT_PD_2 - 2;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_2_ED_2		= BOOK_2_SWAP_2_CASHFLOW_INT_PD_2 - 1;

	public static final int		BOOK_1_SWAP_1_CASHFLOW_DIV_PD_ED_1_AMT		= 1000;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_1_ED_1_AMT	= 10;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_1_ED_2_AMT	= 20;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_2_ED_1_AMT	= 30;
	public static final int		BOOK_1_SWAP_1_CASHFLOW_INT_PD_2_ED_2_AMT	= 40;

	public static final int		BOOK_2_SWAP_1_CASHFLOW_DIV_PD_ED_AMT		= 5000;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_1_ED_1_AMT	= -10;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_1_ED_2_AMT	= -20;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_2_ED_1_AMT	= -30;
	public static final int		BOOK_2_SWAP_1_CASHFLOW_INT_PD_2_ED_2_AMT	= -40;

	public static final int		BOOK_1_SWAP_2_CASHFLOW_DIV_PD_ED_1_AMT		= 25000;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_1_ED_1_AMT	= 5;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_1_ED_2_AMT	= 15;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_2_ED_1_AMT	= 25;
	public static final int		BOOK_1_SWAP_2_CASHFLOW_INT_PD_2_ED_2_AMT	= 35;

	public static final int		BOOK_2_SWAP_2_CASHFLOW_DIV_PD_ED_AMT		= 500;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_1_ED_1_AMT	= -5;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_1_ED_2_AMT	= -15;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_2_ED_1_AMT	= -25;
	public static final int		BOOK_2_SWAP_2_CASHFLOW_INT_PD_2_ED_2_AMT	= -35;

}
