package org.galatea.pochdfs.hdfs;

public class FilepathConstructor {

	private static final FilepathConstructor INSTANCE = new FilepathConstructor();

	private static final String BASE_PATH = "/cs/data/";
	private static final String FILE_EXTENSION = ".jsonl";

	private FilepathConstructor() {
	}

	public static FilepathConstructor getInstance() {
		return INSTANCE;
	}

	public String constructInstRefsFilepath() {
		return buildFilepath(BASE_PATH, "instrument/", BaseFilename.Filename.INST_REFS.getFilename(), FILE_EXTENSION);
	}

	public String constructCounterpartyFilepath() {
		return buildFilepath(BASE_PATH, "counterparty/", BaseFilename.Filename.COUNTERPARTY.getFilename(),
				FILE_EXTENSION);
	}

	public String constructSwapContractFilepath(final int counterPartyId) {
		return buildFilepath(BASE_PATH, "swapcontracts/", String.valueOf(counterPartyId), "-",
				BaseFilename.Filename.SWAP_CONTRACT.getFilename(), FILE_EXTENSION);
	}

	public String constructPositionFilepath(final int swapId, final int effectiveDate) {
		return buildFilepath(BASE_PATH, "positions/", String.valueOf(swapId), "-", String.valueOf(effectiveDate), "-",
				BaseFilename.Filename.POSITIONS.getFilename(), FILE_EXTENSION);
	}

	public String constructCashFlowFilepath(final int swapId) {
		return buildFilepath(BASE_PATH, "cashflows/", String.valueOf(swapId), "-",
				BaseFilename.Filename.CASH_FLOWS.getFilename(), FILE_EXTENSION);
	}

	private String buildFilepath(final String... pathParts) {
		StringBuilder builder = new StringBuilder();
		for (String part : pathParts) {
			builder.append(part);
		}
		return builder.toString();
	}

}
