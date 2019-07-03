package org.galatea.pochdfs.hdfs;

import java.io.File;

public class DatanameExtractor {

	private static final DatanameExtractor INSTANCE = new DatanameExtractor();

	private DatanameExtractor() {

	}

	public static DatanameExtractor getInstance() {
		return INSTANCE;
	}

	public String extract(final File file) {
		String filename = file.getName();
		int extensionIndex = filename.indexOf(".");
		if (extensionIndex != -1) {
			return filename.substring(0, extensionIndex);
		} else {
			return "";
		}
	}
}
