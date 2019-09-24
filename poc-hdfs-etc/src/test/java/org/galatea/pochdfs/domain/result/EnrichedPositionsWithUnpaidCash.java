package org.galatea.pochdfs.domain.result;

import org.apache.spark.sql.Row;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@ToString
@Accessors(fluent = true, chain = true)
public class EnrichedPositionsWithUnpaidCash {

	private int		instId;
	private int		swapId;
	private String	ric;
	private int		counterPartyId;
	private String	counterPartyField1;
	private String	effectiveDate;
	private double	unpaidDiv;
	private double	unpaidInt;

	public boolean equalsRow(final Row row) {
		return (equalsInstId(row) && equalsSwapId(row) && equalsCounterPartyId(row) && equalsCounterPartyField1(row)
				&& equalsEffectiveDate(row) && equalsRic(row) && equalsUnpaidDiv(row) && equalsUnpaidInt(row));
	}

	private boolean equalsInstId(final Row row) {
		Long instId = row.getAs("instrument_id");
		return instId.equals(Long.valueOf(this.instId));
	}

	private boolean equalsSwapId(final Row row) {
		Long swapId = row.getAs("swap_contract_id");
		return swapId.equals(Long.valueOf(this.swapId));
	}

	private boolean equalsCounterPartyId(final Row row) {
		Long counterPartyId = row.getAs("counterparty_id");
		return counterPartyId.equals(Long.valueOf(this.counterPartyId));
	}

	private boolean equalsCounterPartyField1(final Row row) {
		String field = row.getAs("counterparty_field1");
		return field.equalsIgnoreCase(this.counterPartyField1);
	}

	private boolean equalsEffectiveDate(final Row row) {
		String effectiveDate = row.getAs("effective_date");
		return effectiveDate.equals(this.effectiveDate);
	}

	private boolean equalsRic(final Row row) {
		String ric = row.getAs("ric");
		return ric.equalsIgnoreCase(this.ric);
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
