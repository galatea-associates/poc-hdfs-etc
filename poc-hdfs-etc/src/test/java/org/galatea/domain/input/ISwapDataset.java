package org.galatea.domain.input;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface ISwapDataset {

	public static final ObjectMapper MAPPER = new ObjectMapper();

	public void write();

}
