package org.galatea.pochdfs.domain.result;

import org.apache.spark.sql.Row;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@ToString
@Accessors(fluent = true, chain = true)
public class UnpaidCash {

	private int		instId;
	private int		swapId;
	private double	unpaidDiv;
	private double	unpaidInt;

	public boolean equalsRow(final Row row) {
		return (equalsInstId(row) && equalsSwapId(row) && equalsUnpaidDiv(row) && equalsUnpaidInt(row));
	}

	private boolean equalsInstId(final Row row) {
		Long instId = row.getAs("instrument_id");
		return instId.equals(Long.valueOf(this.instId));
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
