package org.galatea.hadooppoc.hdfs;

import lombok.SneakyThrows;

public class FilepathConstructor {

	private static final String BASE_PATH = "/cs/data/";
	private static final String FILE_EXTENSION = ".jsonl";

	private FilepathConstructor() {
	}

	public static FilepathConstructor newConstructor() {
		return new FilepathConstructor();
	}

	public String constructInstRefsFilename() {
		return buildFilepath(BASE_PATH, "instrument/", BaseFilename.Filename.INST_REFS.getFilename(), FILE_EXTENSION);
	}

	public String constructLegalEntityFilename() {
		return buildFilepath(BASE_PATH, "swap-header/", BaseFilename.Filename.LEGAL_ENTITY.getFilename(),
				FILE_EXTENSION);
	}

	public String constructCounterpartyFilename() {
		return buildFilepath(BASE_PATH, "counterparty/", BaseFilename.Filename.COUNTERPARTY.getFilename(),
				FILE_EXTENSION);
	}

	@SneakyThrows
	public String constructSwapHeaderFilename(final String counterPartyId) {
		return buildFilepath(BASE_PATH, "swap-header/", counterPartyId, "-",
				BaseFilename.Filename.SWAP_HEADER.getFilename(), FILE_EXTENSION);
	}

	private String buildFilepath(final String... fileParts) {
		StringBuilder builder = new StringBuilder();
		for (String part : fileParts) {
			builder.append(part);
		}
		return builder.toString();
	}

}
