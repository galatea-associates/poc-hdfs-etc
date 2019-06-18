package org.galatea.pochdfs.hdfs.jsonobjects;

import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
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

	public void addSwapHeader(final SwapHeader header) {
		this.swapHeaders.add(header);
	}

}