package org.galatea.pochdfs.domain.input;

import org.galatea.pochdfs.domain.Defaults;
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
public class Instrument implements SwapDataset {

	private int		instrument_id;
	private String	ric;

	public static Instrument defaultInstrument() {
		return new Instrument().instrument_id(Defaults.INSTRUMENT_ID).ric(Defaults.RIC);
	}

	@Override
	@SneakyThrows
	public void write() {
		String path = "instrument/instruments.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}
