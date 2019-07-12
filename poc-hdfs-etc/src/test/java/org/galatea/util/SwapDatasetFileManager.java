package org.galatea.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.util.FileSystemUtils;

import lombok.Getter;
import lombok.SneakyThrows;

public class SwapDatasetFileManager {

	@Getter
	protected static final String INPUT_BASE_PATH = "src/test/resources/testInputData/swapData/";

//	@SneakyThrows
//	public SwapDatasetFileManager(final String basePath) {
//		this.basePath = basePath;
//		Files.createDirectories(Paths.get(basePath + "temp.txt").getParent());
//	}

	@SneakyThrows
	public static void createDirectory() {
		Files.createDirectories(Paths.get(INPUT_BASE_PATH + "temp.txt").getParent());
	}

	@SneakyThrows
	public static void writeToFile(final String path, final String json) {
		String filepath = INPUT_BASE_PATH + path;
		Files.createDirectories(Paths.get(filepath).getParent());
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filepath, true))) {
			writer.write(json);
		}
	}

//	@SneakyThrows
//	private static void createFile(final String filepath, final String json) {
//		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filepath))) {
//			writer.write(json);
//		}
//	}
//
//	@SneakyThrows
//	private static void appendFile(final String filepath, final String json) {
//		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filepath, ))) {
//			writer.write(json);
//		}
//	}

	@SneakyThrows
	public static void deleteData() {
		FileSystemUtils.deleteRecursively(Paths.get(INPUT_BASE_PATH));
	}

}
