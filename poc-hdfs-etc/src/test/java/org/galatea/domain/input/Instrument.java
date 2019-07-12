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
public class Instrument implements ISwapDataset {

	private int time_stamp = 20190101;
	private int instrument_id;
	private String ric;

	@Override
	@SneakyThrows
	public void write() {
		String path = "instrument/instruments.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}
