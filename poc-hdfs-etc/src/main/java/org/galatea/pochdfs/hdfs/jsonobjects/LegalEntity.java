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
public class LegalEntity {

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

	private String code;
	private String name;
	private int timeStamp = Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString());

}