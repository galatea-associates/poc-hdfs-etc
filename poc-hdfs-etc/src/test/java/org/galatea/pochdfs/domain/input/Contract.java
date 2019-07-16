package org.galatea.pochdfs.domain.input;

import org.galatea.pochdfs.util.SwapDatasetFileManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

@NoArgsConstructor
@Setter
@Accessors(fluent = true, chain = true)
@JsonAutoDetect(fieldVisibility = Visibility.ANY)
@SuppressWarnings("unused")
public class Contract implements ISwapDataset {

	public static int DEFAULT_CONTRACT_ID = 12345;
	public static int DEFAULT_COUNTERPARTY_ID = 200;

	private int time_stamp = 20190101;
	private int swap_contract_id;
	private int counterparty_id;

	@Override
	@SneakyThrows
	public void write() {
		String path = "swapcontracts/" + counterparty_id + "-swapContracts.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

	public static Contract defaultContract() {
		return new Contract().swap_contract_id(DEFAULT_CONTRACT_ID).counterparty_id(DEFAULT_COUNTERPARTY_ID);
	}

}
