package org.galatea.pochdfs.hdfs;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class BaseFilename {

	private BaseFilename() {

	}

	@AllArgsConstructor
	@Getter
	public static enum Filename {
		COUNTERPARTY("counterparties"), LEGAL_ENTITY("legalEntity"), INST_REFS("instruments"),
		SWAP_CONTRACT("swapContracts"), POSITIONS("positions"), CASH_FLOWS("cashFlows");
		private String filename;
	}

}
