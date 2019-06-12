package org.galatea.hadooppoc.spark;

import java.util.ArrayList;
import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class SwapDataFiles {

	private String path;
	private Collection<SwapDataFilename> filenames;

	public static SwapDataFiles newIncludeAllSwapDataFiles(final String path) {
		return new SwapDataFiles(path, generateSwapDataFilenames());
	}

	private static Collection<SwapDataFilename> generateSwapDataFilenames() {
		Collection<SwapDataFilename> filenames = new ArrayList<>();
		for (SwapDataFilename filename : SwapDataFilename.values()) {
			filenames.add(filename);
		}
		return filenames;
	}

	@AllArgsConstructor
	@Getter
	public static enum SwapDataFilename {
		SWAP_HEADER_200("200-swapHeader"), SWAP_HEADER_201("201-swapHeader"), LEGAL_ENTITY("legalEntity"),
		INSTRUMENTS("instruments"), COUNTER_PARTIES("counterparties");

		private String filename;
	}

}
