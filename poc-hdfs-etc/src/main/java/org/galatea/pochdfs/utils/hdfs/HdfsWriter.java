package org.galatea.pochdfs.utils.hdfs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class HdfsWriter implements IHdfsWriter {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final FileSystem fileSystem;

	@Override
	@SneakyThrows
	public void createFile(final Path path, final Object object) {
		createFileFromByteArray(path, createByteArray(object));
	}

	@SneakyThrows
	private void createFileFromByteArray(final Path path, final byte[] source) {
		try (InputStream inputStream = new ByteArrayInputStream(source);
				FSDataOutputStream outputStream = fileSystem.create(path)) {
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = inputStream.read(b)) > 0) {
				outputStream.write(b, 0, numBytes);
			}
		}
	}

	@Override
	@SneakyThrows
	public void appendFile(final Path path, final Object object) {
		appendByteArrayToFile(path, createByteArray(object));
	}

	@SneakyThrows
	private void appendByteArrayToFile(final Path path, final byte[] source) {
		try (FSDataOutputStream outputStream = fileSystem.append(path);
				InputStream inputStream = new ByteArrayInputStream(source)) {
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = inputStream.read(b)) > 0) {
				outputStream.write(b, 0, numBytes);
			}

		}
	}

	@SneakyThrows
	private byte[] createByteArray(final Object object) {
		StringBuilder builder = new StringBuilder(MAPPER.writeValueAsString(object));
		return builder.append("\n").toString().getBytes();
	}

	@Override
	@SneakyThrows
	public boolean fileExists(final Path path) {
		return fileSystem.exists(path);
	}

}
