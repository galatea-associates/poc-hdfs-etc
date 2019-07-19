package org.galatea.pochdfs.data;

import org.galatea.pochdfs.domain.input.CashFlow;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.input.Instrument;
import org.galatea.pochdfs.domain.input.Position;

public class MegaTestData {

	public static final String	BOOK_1											= "AABBCC";
	public static final String	BOOK_2											= "DDEEFF";

	public static final int		BOOK_1_CP_ID									= 200;
	public static final int		BOOK_2_CP_ID									= 201;

	public static final String	BOOK_1_CP_FIELD									= "BOOK1";
	public static final String	BOOK_2_CP_FIELD									= "BOOK2";

	public static final int		BOOK_1_SWAP_1_ID								= 101;
	public static final int		BOOK_1_SWAP_2_ID								= 102;
	public static final int		BOOK_2_SWAP_1_ID								= 201;
	public static final int		BOOK_2_SWAP_2_ID								= 202;

	public static final String	INSTRUMENT_1_RIC								= "ABC";
	public static final String	INSTRUMENT_2_RIC								= "DEF";
	public static final String	INSTRUMENT_3_RIC								= "GHI";
	public static final String	INSTRUMENT_4_RIC								= "JKL";
	public static final String	INSTRUMENT_5_RIC								= "MNO";

	public static final int		INSTRUMENT_1_ID									= 11;
	public static final int		INSTRUMENT_2_ID									= 22;
	public static final int		INSTRUMENT_3_ID									= 33;
	public static final int		INSTRUMENT_4_ID									= 44;
	public static final int		INSTRUMENT_5_ID									= 55;

	public static final String	BOOK_1_SWAP_1_POSTITION_1_RIC					= INSTRUMENT_1_RIC;
	public static final String	BOOK_1_SWAP_1_POSTITION_2_RIC					= INSTRUMENT_2_RIC;
	public static final String	BOOK_1_SWAP_1_POSTITION_3_RIC					= INSTRUMENT_3_RIC;
	public static final String	BOOK_1_SWAP_2_POSTITION_1_RIC					= INSTRUMENT_1_RIC;
	public static final String	BOOK_1_SWAP_2_POSTITION_2_RIC					= INSTRUMENT_4_RIC;
	public static final String	BOOK_2_SWAP_1_POSTITION_1_RIC					= INSTRUMENT_5_RIC;
	public static final String	BOOK_2_SWAP_2_POSTITION_2_RIC					= INSTRUMENT_5_RIC;
	public static final String	BOOK_2_SWAP_2_POSTITION_1_RIC					= INSTRUMENT_1_RIC;

	public static final int		BOOK_1_SWAP_1_POSTITION_1_INST_ID				= INSTRUMENT_1_ID;
	public static final int		BOOK_1_SWAP_1_POSTITION_2_INST_ID				= INSTRUMENT_2_ID;
	public static final int		BOOK_1_SWAP_1_POSTITION_3_INST_ID				= INSTRUMENT_3_ID;
	public static final int		BOOK_1_SWAP_2_POSTITION_1_INST_ID				= INSTRUMENT_1_ID;
	public static final int		BOOK_1_SWAP_2_POSTITION_2_INST_ID				= INSTRUMENT_4_ID;
	public static final int		BOOK_2_SWAP_1_POSTITION_1_INST_ID				= INSTRUMENT_5_ID;
	public static final int		BOOK_2_SWAP_2_POSTITION_2_INST_ID				= INSTRUMENT_5_ID;
	public static final int		BOOK_2_SWAP_2_POSTITION_1_INST_ID				= INSTRUMENT_1_ID;

	public static final int		BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_S			= 111;
	public static final int		BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_E			= 222;
	public static final int		BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_I			= 333;

	public static final int		BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_S			= 444;
	public static final int		BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_E			= 555;
	public static final int		BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_I			= 666;

	public static final int		BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_S			= 777;
	public static final int		BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_E			= 888;
	public static final int		BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_I			= 999;

	public static final int		BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_S			= 987;
	public static final int		BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_E			= 654;
	public static final int		BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_I			= 321;

	public static final int		BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_S			= 445;
	public static final int		BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_E			= 335;
	public static final int		BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_I			= 225;

	public static final int		BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_S			= 109;
	public static final int		BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_E			= 876;
	public static final int		BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_I			= 543;

	public static final int		BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_S			= 210;
	public static final int		BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_E			= 234;
	public static final int		BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_I			= 567;

	public static final int		BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_S			= 890;
	public static final int		BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_E			= 123;
	public static final int		BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_I			= 456;

	public static final String	BOOK_1_CASHFLOW_DIV_PD							= "2019-01-06";
	public static final String	BOOK_1_CASHFLOW_INT_PD_1						= "2019-01-03";
	public static final String	BOOK_1_CASHFLOW_INT_PD_2						= "2019-01-05";

	public static final String	BOOK_2_CASHFLOW_DIV_PD							= "2019-01-07";
	public static final String	BOOK_2_CASHFLOW_INT_PD_1						= "2019-01-04";
	public static final String	BOOK_2_CASHFLOW_INT_PD_2						= "2019-01-06";

	public static final String	BOOK_1_CASHFLOW_DIV_ED							= "2019-01-01";
	public static final String	BOOK_1_CASHFLOW_INT_PD_1_ED_1					= "2019-01-01";
	public static final String	BOOK_1_CASHFLOW_INT_PD_1_ED_2					= "2019-01-02";
	public static final String	BOOK_1_CASHFLOW_INT_PD_2_ED_1					= "2019-01-03";
	public static final String	BOOK_1_CASHFLOW_INT_PD_2_ED_2					= "2019-01-04";

	public static final String	BOOK_2_CASHFLOW_DIV_ED							= "2019-01-02";
	public static final String	BOOK_2_CASHFLOW_INT_PD_1_ED_1					= "2019-01-02";
	public static final String	BOOK_2_CASHFLOW_INT_PD_1_ED_2					= "2019-01-03";
	public static final String	BOOK_2_CASHFLOW_INT_PD_2_ED_1					= "2019-01-04";
	public static final String	BOOK_2_CASHFLOW_INT_PD_2_ED_2					= "2019-01-05";

	public static final int		BOOK_1_SWAP_1_POS_1_CASHFLOW_DIV_PD_ED_AMT		= 1000;
	public static final int		BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT	= 10;
	public static final int		BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT	= 20;
	public static final int		BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT	= 30;
	public static final int		BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT	= 40;

	public static final int		BOOK_1_SWAP_1_POS_2_CASHFLOW_DIV_PD_ED_AMT		= 15000;
	public static final int		BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT	= 150;
	public static final int		BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT	= 250;
	public static final int		BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT	= 350;
	public static final int		BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT	= 450;

	public static final int		BOOK_1_SWAP_1_POS_3_CASHFLOW_DIV_PD_ED_AMT		= 150001;
	public static final int		BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_1_ED_1_AMT	= 1501;
	public static final int		BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_1_ED_2_AMT	= 2501;
	public static final int		BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_2_ED_1_AMT	= 3501;
	public static final int		BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_2_ED_2_AMT	= 4501;

	public static final int		BOOK_2_SWAP_1_POS_1_CASHFLOW_DIV_PD_ED_AMT		= 5000;
	public static final int		BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT	= -10;
	public static final int		BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT	= -20;
	public static final int		BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT	= -30;
	public static final int		BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT	= -40;

	public static final int		BOOK_1_SWAP_2_POS_1_CASHFLOW_DIV_PD_ED_AMT		= 25000;
	public static final int		BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT	= 5;
	public static final int		BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT	= 15;
	public static final int		BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT	= 25;
	public static final int		BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT	= 35;

	public static final int		BOOK_1_SWAP_2_POS_2_CASHFLOW_DIV_PD_ED_AMT		= 250050;
	public static final int		BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT	= 55;
	public static final int		BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT	= 155;
	public static final int		BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT	= 255;
	public static final int		BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT	= 355;

	public static final int		BOOK_2_SWAP_2_POS_1_CASHFLOW_DIV_PD_ED_AMT		= 500;
	public static final int		BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT	= -5;
	public static final int		BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT	= -15;
	public static final int		BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT	= -25;
	public static final int		BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT	= -35;

	public static final int		BOOK_2_SWAP_2_POS_2_CASHFLOW_DIV_PD_ED_AMT		= 5500;
	public static final int		BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT	= -55;
	public static final int		BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT	= -515;
	public static final int		BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT	= -525;
	public static final int		BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT	= -535;

	public static final String	BOOK_1_SWAP_POS_ED_1							= "2019-01-02";
	public static final String	BOOK_1_SWAP_POS_ED_2							= "2019-01-03";
	public static final String	BOOK_2_SWAP_POS_ED_1							= "2019-01-03";
	public static final String	BOOK_2_SWAP_POS_ED_2							= "2019-01-04";

	public static void writeCounterParties() {
		CounterParty.defaultCounterParty().book(BOOK_1).counterparty_id(BOOK_1_CP_ID)
				.counterparty_field1(BOOK_1_CP_FIELD).write();
		CounterParty.defaultCounterParty().book(BOOK_2).counterparty_id(BOOK_2_CP_ID)
				.counterparty_field1(BOOK_2_CP_FIELD).write();
	}

	public static void writeSwapContracts() {
		Contract.defaultContract().counterparty_id(BOOK_1_CP_ID).swap_contract_id(BOOK_1_SWAP_1_ID).write();
		Contract.defaultContract().counterparty_id(BOOK_1_CP_ID).swap_contract_id(BOOK_1_SWAP_2_ID).write();
		Contract.defaultContract().counterparty_id(BOOK_2_CP_ID).swap_contract_id(BOOK_2_SWAP_1_ID).write();
		Contract.defaultContract().counterparty_id(BOOK_2_CP_ID).swap_contract_id(BOOK_2_SWAP_2_ID).write();
	}

	public static void writeInstruments() {
		Instrument.defaultInstrument().ric(INSTRUMENT_1_RIC).ric(INSTRUMENT_1_RIC).write();
		Instrument.defaultInstrument().ric(INSTRUMENT_2_RIC).ric(INSTRUMENT_2_RIC).write();
		Instrument.defaultInstrument().ric(INSTRUMENT_3_RIC).ric(INSTRUMENT_3_RIC).write();
		Instrument.defaultInstrument().ric(INSTRUMENT_4_RIC).ric(INSTRUMENT_4_RIC).write();
		Instrument.defaultInstrument().ric(INSTRUMENT_5_RIC).ric(INSTRUMENT_5_RIC).write();
	}

	public static void writePositions() {
		writeBook1Swap1ED1Positions();
		writeBook1Swap1ED2Positions();
		writeBook1Swap2ED1Positions();
		writeBook1Swap2ED2Positions();
		writeBook2Swap1ED1Positions();
		writeBook2Swap1ED2Positions();
		writeBook2Swap2ED1Positions();
		writeBook2Swap2ED2Positions();
	}

	private static void writeBook1Swap1ED1Positions() {
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();

		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_2_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_2_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_2_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();

		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_3_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_3_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_3_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
	}

	private static void writeBook1Swap1ED2Positions() {
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();

		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_2_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_2_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_2_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_2_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();

		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_3_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_3_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_1_POSTITION_3_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_1_POSTITION_3_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_1_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
	}

	private static void writeBook1Swap2ED1Positions() {
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();

		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_2_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_2_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_2_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_1).write();
	}

	private static void writeBook1Swap2ED2Positions() {
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();

		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_2_RIC).position_type("S")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_S).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_2_RIC).position_type("E")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_E).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_1_SWAP_2_POSTITION_2_RIC).position_type("I")
				.td_quantity(BOOK_1_SWAP_2_POSTITION_2_RIC_TD_QTY_I).swap_contract_id(BOOK_1_SWAP_2_ID)
				.effective_date(BOOK_1_SWAP_POS_ED_2).write();
	}

	private static void writeBook2Swap1ED1Positions() {
		Position.defaultPosition().ric(BOOK_2_SWAP_1_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_2_SWAP_1_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_1_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_2_SWAP_1_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_1_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_2_SWAP_1_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
	}

	private static void writeBook2Swap1ED2Positions() {
		Position.defaultPosition().ric(BOOK_2_SWAP_1_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_2_SWAP_1_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_1_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_2_SWAP_1_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_1_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_2_SWAP_1_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_2_SWAP_1_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
	}

	private static void writeBook2Swap2ED1Positions() {
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();

		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_2_RIC).position_type("S")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_S).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_2_RIC).position_type("E")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_E).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_2_RIC).position_type("I")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_I).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_1).write();
	}

	private static void writeBook2Swap2ED2Positions() {
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_1_RIC).position_type("S")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_S).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_1_RIC).position_type("E")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_E).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_1_RIC).position_type("I")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_1_RIC_TD_QTY_I).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();

		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_2_RIC).position_type("S")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_S).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_2_RIC).position_type("E")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_E).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
		Position.defaultPosition().ric(BOOK_2_SWAP_2_POSTITION_2_RIC).position_type("I")
				.td_quantity(BOOK_2_SWAP_2_POSTITION_2_RIC_TD_QTY_I).swap_contract_id(BOOK_2_SWAP_2_ID)
				.effective_date(BOOK_2_SWAP_POS_ED_2).write();
	}

	public static void writeCashFlows() {
		writeBook1Swap1Pos1Cashflows();
		writeBook1Swap1Pos2Cashflows();
		writeBook1Swap1Pos3Cashflows();
		writeBook1Swap2Pos1Cashflows();
		writeBook1Swap2Pos2Cashflows();
		writeBook2Swap1Pos1Cashflows();
		writeBook2Swap2Pos1Cashflows();
		writeBook2Swap2Pos2Cashflows();
	}

	private static void writeBook1Swap1Pos1Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_DIV_ED).pay_date(BOOK_1_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_1_SWAP_1_POS_1_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook1Swap1Pos2Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_DIV_ED).pay_date(BOOK_1_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_1_SWAP_1_POS_2_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook1Swap1Pos3Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_3_RIC)
				.effective_date(BOOK_1_CASHFLOW_DIV_ED).pay_date(BOOK_1_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_1_SWAP_1_POS_3_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_3_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_3_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_3_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_1_ID).ric(BOOK_1_SWAP_1_POSTITION_3_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_1_POS_3_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook1Swap2Pos1Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_DIV_ED).pay_date(BOOK_1_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_1_SWAP_2_POS_1_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook1Swap2Pos2Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_DIV_ED).pay_date(BOOK_1_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_1_SWAP_2_POS_2_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_1_SWAP_2_ID).ric(BOOK_1_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_1_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_1_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_1_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook2Swap1Pos1Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_1_ID).ric(BOOK_2_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_DIV_ED).pay_date(BOOK_2_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_2_SWAP_1_POS_1_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_1_ID).ric(BOOK_2_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_2_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_1_ID).ric(BOOK_2_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_2_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_1_ID).ric(BOOK_2_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_2_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_1_ID).ric(BOOK_2_SWAP_1_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_2_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_2_SWAP_1_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook2Swap2Pos1Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_DIV_ED).pay_date(BOOK_2_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_2_SWAP_2_POS_1_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_2_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_2_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_2_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_1_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_2_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_1_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}

	private static void writeBook2Swap2Pos2Cashflows() {
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_2_CASHFLOW_DIV_ED).pay_date(BOOK_2_CASHFLOW_DIV_PD).cashflow_type("DIV")
				.amount(BOOK_2_SWAP_2_POS_2_CASHFLOW_DIV_PD_ED_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_1_ED_1).pay_date(BOOK_2_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_1_ED_2).pay_date(BOOK_2_CASHFLOW_INT_PD_1).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_1_ED_2_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_2_ED_1).pay_date(BOOK_2_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_1_AMT).write();
		CashFlow.defaultCashFlow().swap_contract_id(BOOK_2_SWAP_2_ID).ric(BOOK_2_SWAP_2_POSTITION_2_RIC)
				.effective_date(BOOK_2_CASHFLOW_INT_PD_2_ED_2).pay_date(BOOK_2_CASHFLOW_INT_PD_2).cashflow_type("INT")
				.amount(BOOK_2_SWAP_2_POS_2_CASHFLOW_INT_PD_2_ED_2_AMT).write();
	}
}
