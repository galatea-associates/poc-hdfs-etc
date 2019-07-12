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
public class CounterParty implements ISwapDataset {

	private int counterparty_id;
	private String counterparty_field1;

	@Override
	@SneakyThrows
	public void write() {
		String path = "counterparty/counterparties.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}