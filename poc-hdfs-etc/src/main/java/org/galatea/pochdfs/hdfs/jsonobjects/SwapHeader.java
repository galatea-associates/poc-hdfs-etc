package org.galatea.pochdfs.hdfs.jsonobjects;

import java.time.LocalDateTime;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SwapHeader {

	private String counterPartyId;
	private String swapId;
	private String swapMnemonic;
	private String isShortMtmFinanced;
	private String timeStamp = LocalDateTime.now().toString();

}
