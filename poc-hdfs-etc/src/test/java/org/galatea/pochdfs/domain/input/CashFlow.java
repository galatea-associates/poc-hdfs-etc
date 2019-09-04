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
public class CashFlow implements SwapDataset {

	private int		cashflow_id;
	private double	amount;
	private String	long_short;
	private int		swap_contract_id;
	// private int instrument_id;
	private String	ric;
	private String	currency;
	private String	cashflow_type;
	private String	effective_date;
	// private int pay_date;
	private String	pay_date;

	public static CashFlow defaultCashFlow() {
		return new CashFlow().cashflow_id(Defaults.CASHFLOW_ID).amount(Defaults.AMOUNT).long_short(Defaults.LONG_SHORT)
				.swap_contract_id(Defaults.CONTRACT_ID).ric(Defaults.RIC).currency(Defaults.CURRENCY)
				.cashflow_type(Defaults.CASHFLOW_TYPE).effective_date(Defaults.EFFECTIVE_DATE)
				.pay_date(Defaults.PAYDATE);
	}

	@Override
	@SneakyThrows
	public void write() {
		String path = "cashflows/" + getYearAndMonth(effective_date) + "-" + getYearAndMonth(pay_date) + "-" + swap_contract_id + "-cashFlows.jsonl";
		System.out.println(path);
		String json = MAPPER.writeValueAsString(this) + "\n";
		SwapDatasetFileManager.writeToFile(path, json);
	}
	private String getYearAndMonth(String date){
		String newDate = date.replaceAll("-","");
		return newDate.substring(0,6);
	}

}
