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
public class Position {

	private String swapId;
	private String instrumentId;
	private String cobDate;
	private String counterPartyId;
	private String timeStamp = LocalDateTime.now().toString();

}