package org.galatea.pochdfs.hdfs.jsonobjects;

import java.time.LocalDateTime;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class CashFlow {

	private String swapId;
	private String instrumentId;
	private String type;
	private String payDate;
	private String effectiveDate;
	private String currency;
	private String amount;
	private String timeStamp = LocalDateTime.now().toString();

}
