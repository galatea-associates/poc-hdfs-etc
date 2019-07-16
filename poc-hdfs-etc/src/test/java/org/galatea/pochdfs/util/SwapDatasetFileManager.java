package org.galatea.pochdfs.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.util.FileSystemUtils;

import lombok.Getter;
import lombok.SneakyThrows;

public class SwapDatasetFileManager {

	@Getter
	private static final String INPUT_BASE_PATH = "src/test/resources/testInputData/swapData/";

	@SneakyThrows
	public static void writeToFile(final String path, final String json) {
		String filepath = INPUT_BASE_PATH + path;
		Files.createDirectories(Paths.get(filepath).getParent());
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filepath, true))) {
			writer.write(json);
		}
	}

	@SneakyThrows
	public static void deleteData() {
		FileSystemUtils.deleteRecursively(Paths.get(INPUT_BASE_PATH));
	}

}
