package org.galatea.pochdfs.service.writer;

import org.springframework.stereotype.Service;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Service
public class SwapBaseFilename {

	private SwapBaseFilename() {

	}

	@AllArgsConstructor
	@Getter
	public static enum Filename {
		COUNTERPARTY("counterparties"), LEGAL_ENTITY("legalEntity"), INST_REFS("instruments"),
		SWAP_CONTRACT("swapContracts"), POSITIONS("positions"), CASH_FLOWS("cashFlows");
		private String filename;
	}

}
