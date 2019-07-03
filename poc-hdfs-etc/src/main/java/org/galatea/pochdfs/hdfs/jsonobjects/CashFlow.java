package org.galatea.pochdfs.hdfs.jsonobjects;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CashFlow {

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddss");

	private int cashFlowId;
	private int swapId;
	private int instrumentId;
	private String type;
	private int payDate;
	private int effectiveDate;
	private String currency;
	private double amount;
	private String longShort;
	private int timeStamp = Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString());

}
