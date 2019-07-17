package org.galatea.pochdfs.domain.result;

import org.apache.spark.sql.Row;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@ToString
@Accessors(fluent = true, chain = true)
public class EnrichedPosition {

	private int		instId;
	private int		swapId;
	private String	ric;
	private int		counterPartyId;
	private String	counterPartyField1;
	private int		effectiveDate;
	private int		tdQuantity;
	private String	book;

	public boolean equalsRow(final Row row) {
		return (equalsInstId(row) && equalsSwapId(row) && equalsCounterPartyId(row) && equalsCounterPartyField1(row)
				&& equalsEffectiveDate(row) && equalsRic(row) && equalsTdQuantity(row) && equalsBook(row));
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
		return field.equals(this.counterPartyField1);
	}

	private boolean equalsEffectiveDate(final Row row) {
		Long effectiveDate = row.getAs("effective_date");
		return effectiveDate.equals(Long.valueOf(this.effectiveDate));
	}

	private boolean equalsRic(final Row row) {
		String ric = row.getAs("ric");
		return ric.equalsIgnoreCase(this.ric);
	}

	private boolean equalsTdQuantity(final Row row) {
		Long tdQuantity = row.getAs("td_quantity");
		return tdQuantity.equals(Long.valueOf(this.tdQuantity));
	}

	private boolean equalsBook(final Row row) {
		String book = row.getAs("book");
		return book.equals(this.book);
	}
}