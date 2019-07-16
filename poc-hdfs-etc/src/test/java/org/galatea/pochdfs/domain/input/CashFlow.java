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
public class CashFlow implements ISwapDataset {

	private int time_stamp = 20190101;
	private int cashflow_id;
	private double amount;
	private String long_short;
	private int swap_contract_id;
	private int instrument_id;
	private String currency;
	private String cashflow_type;
	private int effective_date;
	private int pay_date;

	@Override
	@SneakyThrows
	public void write() {
		String path = "cashflows/" + swap_contract_id + "-cashFlows.jsonl";
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}

}
