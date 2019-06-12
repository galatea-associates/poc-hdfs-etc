package org.galatea.hadooppoc.hdfs;

import java.io.File;

public class DatanameExtractor {

	private DatanameExtractor() {

	}

	public static DatanameExtractor newExtractor() {
		return new DatanameExtractor();
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
