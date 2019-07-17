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
public class CounterParty implements SwapDataset {

	private String	book;
	private int		counterparty_id;
	private String	counterparty_field1;

	public static CounterParty defaultCounterParty() {
		return new CounterParty().counterparty_id(Defaults.COUNTERPARTY_ID)
				.counterparty_field1(Defaults.COUNTERPARTY_FIELD1).book(Defaults.BOOK);
	}

	@Override
	@SneakyThrows
	public void write() {
		String path = "counterparty/counterparties.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}