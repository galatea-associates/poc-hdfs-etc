package org.galatea.pochdfs.domain.result;

import org.apache.spark.sql.Row;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@ToString
@Accessors(fluent = true, chain = true)
public class UnpaidCash {

	private String		ric;
	private int		swapId;
	private double	unpaidDiv;
	private double	unpaidInt;

	public boolean equalsRow(final Row row) {
		return (equalsRic(row) && equalsSwapId(row) && equalsUnpaidDiv(row) && equalsUnpaidInt(row));
	}

	private boolean equalsRic(final Row row) {
		String ric = row.getAs("ric");
		return ric.equals(this.ric);
	}

	private boolean equalsSwapId(final Row row) {
		Long swapId = row.getAs("swap_contract_id");
		return swapId.equals(Long.valueOf(this.swapId));
	}

	private boolean equalsUnpaidDiv(final Row row) {
		double unpaidDiv = row.getAs("unpaid_DIV");
		return unpaidDiv == this.unpaidDiv;
	}

	private boolean equalsUnpaidInt(final Row row) {
		double unpaidInt = row.getAs("unpaid_INT");
		return unpaidInt == this.unpaidInt;
	}

}
