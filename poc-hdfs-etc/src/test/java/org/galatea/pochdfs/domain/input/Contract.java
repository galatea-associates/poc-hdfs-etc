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
public class Contract implements SwapDataset {

	private int	swap_contract_id;
	private int	counterparty_id;

	public static Contract defaultContract() {
		return new Contract().swap_contract_id(Defaults.CONTRACT_ID).counterparty_id(Defaults.COUNTERPARTY_ID);
	}

	@Override
	@SneakyThrows
	public void write() {
		String path = "swapcontracts/" + counterparty_id + "-swapContracts.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}
