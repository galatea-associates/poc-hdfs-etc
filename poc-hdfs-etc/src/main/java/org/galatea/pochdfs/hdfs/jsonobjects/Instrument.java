package org.galatea.pochdfs.hdfs.jsonobjects;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Instrument {

	private String ric;
	private String instrumentId;
	private String timeStamp = LocalDateTime.now().toString();

}
