package org.galatea.hadooppoc.hdfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.Getter;
import lombok.SneakyThrows;

@Getter
public class FileWriter {

	private FileSystem fileSystem;

	@SneakyThrows
	private FileWriter(final FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public static FileWriter newFileWriter(final FileSystem fileSystem) {
		return new FileWriter(fileSystem);
	}

	@SneakyThrows
	public void createFile(final Path path, final File file) {
		createFile(path, Files.readAllBytes(file.toPath()));
	}

	@SneakyThrows
	public void appendFile(final Path path, final File file) {
		appendFile(path, Files.readAllBytes(file.toPath()));
	}

	@SneakyThrows
	public void createFile(final Path path, final byte[] source) {
		try (InputStream inputStream = new ByteArrayInputStream(source);
				FSDataOutputStream outputStream = fileSystem.create(path)) {
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = inputStream.read(b)) > 0) {
				outputStream.write(b, 0, numBytes);
			}
		}
	}

	@SneakyThrows
	public void appendFile(final Path path, final byte[] source) {
		try (FSDataOutputStream outputStream = fileSystem.append(path);
				InputStream inputStream = new ByteArrayInputStream(source)) {
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = inputStream.read(b)) > 0) {
				outputStream.write(b, 0, numBytes);
			}

		}
	}

}
