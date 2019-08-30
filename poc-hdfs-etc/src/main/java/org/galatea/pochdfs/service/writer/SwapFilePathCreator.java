package org.galatea.pochdfs.service.writer;

import org.springframework.stereotype.Service;

@Service
public class SwapFilePathCreator {

//	private static final SwapFilePathCreator	INSTANCE		= new SwapFilePathCreator();

	private static final String	BASE_PATH		= "/cs/data/";
	private static final String	FILE_EXTENSION	= ".jsonl";

//	private SwapFilePathCreator() {
//	}

//	public static SwapFilePathCreator getInstance() {
//		return INSTANCE;
//	}

	/**
	 *
	 * @return the HDFS file path for instruments
	 */
	public String createInstrumentsFilepath() {
		return buildFilepath(BASE_PATH, "instrument/", SwapBaseFilename.Filename.INST_REFS.getFilename(),
				FILE_EXTENSION);
	}

	/**
	 *
	 * @return the HDFS file path for counter parties
	 */
	public String createCounterpartyFilepath() {
		return buildFilepath(BASE_PATH, "counterparty/", SwapBaseFilename.Filename.COUNTERPARTY.getFilename(),
				FILE_EXTENSION);
	}

	/**
	 *
	 * @param counterPartyId the counter party id
	 * @return the HDFS file path for the counter party's swap contracts
	 */
	public String constructSwapContractFilepath(final int counterPartyId) {
		return buildFilepath(BASE_PATH, "swapcontracts/", String.valueOf(counterPartyId), "-",
				SwapBaseFilename.Filename.SWAP_CONTRACT.getFilename(), FILE_EXTENSION);
	}

	/**
	 *
	 * @param swapId the position's swap id
	 * @return the HDFS file path for the position
	 */
	public String createPositionFilepath( final String effectiveDate, final int swapId) {
		return buildFilepath(BASE_PATH, "positions/",effectiveDate,"-", String.valueOf(swapId), "-",
				SwapBaseFilename.Filename.POSITIONS.getFilename(), FILE_EXTENSION);
	}

	/**
	 *
	 * @param swapId the swap id the cash flow is associated with
	 * @return the HDFS file path for the cash flow
	 */
	public String createCashFlowFilepath(String effectiveDate, String payDate, final int swapId) {
		return buildFilepath(BASE_PATH, "cashflows/", getMonthAndDay(effectiveDate),"-",getMonthAndDay(payDate),"-",String.valueOf(swapId), "-",
				SwapBaseFilename.Filename.CASH_FLOWS.getFilename(), FILE_EXTENSION);
	}

	private String buildFilepath(final String... pathParts) {
		StringBuilder builder = new StringBuilder();
		for (String part : pathParts) {
			builder.append(part);
		}
		return builder.toString();
	}

	private String getMonthAndDay(String date){
		date = date.replaceAll("-","");
		date = date.substring(0,6);
		return date;
	}

}
