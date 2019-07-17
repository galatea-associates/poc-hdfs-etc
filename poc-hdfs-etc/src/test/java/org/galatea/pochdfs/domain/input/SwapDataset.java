package org.galatea.pochdfs.domain.input;

import com.fasterxml.jackson.databind.ObjectMapper;

@FunctionalInterface
public interface SwapDataset {

	public static final ObjectMapper MAPPER = new ObjectMapper();

	public abstract void write();

}
