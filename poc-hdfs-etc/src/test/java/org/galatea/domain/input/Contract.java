package org.galatea.domain.input;

import org.galatea.util.SwapDatasetFileManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true, chain = true)
@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class Contract implements ISwapDataset {

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

}
