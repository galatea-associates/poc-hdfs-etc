package org.galatea.hadooppoc.hdfs.jsonobjects;

import java.util.Collection;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SwapHeaders implements JsonObject {

	Collection<SwapHeader> swapHeaders;

	@Override
	public String getObjectType() {
		return "swapHeaders";
	}

	@Override
	public Collection<SwapHeader> getData() {
		return swapHeaders;
	}

}