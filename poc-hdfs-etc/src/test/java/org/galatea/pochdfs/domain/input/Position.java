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
public class Position implements ISwapDataset {

	private int time_stamp = 20190101;
	private String position_type;
	private int swap_contract_id;
	private int effective_date;
	private String ric;

	@Override
	@SneakyThrows
	public void write() {
		String path = "positions/" + swap_contract_id + "-" + effective_date + "-positions.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}
