package org.galatea.pochdfs.utils.hdfs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Component
public class HdfsWriter {

	private static final ObjectMapper	MAPPER	= new ObjectMapper();

	private final FileSystem			fileSystem;

	// @Override
	@SneakyThrows
	public void createFile(final Path path, final Object object) {
		createFileFromByteArray(path, createByteArray(object));
	}

	// @Override
	@SneakyThrows
	public void createFileFromByteArray(final Path path, final byte[] source) {
		log.info("Create file from byte array");
		Long startTime = System.currentTimeMillis();
		try (InputStream inputStream = new ByteArrayInputStream(source);
				FSDataOutputStream outputStream = fileSystem.create(path)) {
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = inputStream.read(b)) > 0) {
				outputStream.write(b, 0, numBytes);
			}
		}
		log.info("Finished in {} ms", System.currentTimeMillis() - startTime);
	}

	// @Override
	@SneakyThrows
	public void appendFile(final Path path, final Object object) {
		appendByteArrayToFile(path, createByteArray(object));
	}

	// @Override
	@SneakyThrows
	public void appendByteArrayToFile(final Path path, final byte[] source) {
		log.info("Apppending byte array to file");
		Long startTime = System.currentTimeMillis();
		try (FSDataOutputStream outputStream = fileSystem.append(path);
				InputStream inputStream = new ByteArrayInputStream(source)) {
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = inputStream.read(b)) > 0) {
				outputStream.write(b, 0, numBytes);
			}
		}
		log.info("Finished in {} ms", System.currentTimeMillis() - startTime);
	}

	@SneakyThrows
	private byte[] createByteArray(final Object object) {
		log.info("Creating byte array out of object");
		Long startTime = System.currentTimeMillis();
		StringBuilder builder = new StringBuilder(MAPPER.writeValueAsString(object));
		byte[] result = builder.append("\n").toString().getBytes();
		log.info("Byte array creation finished in {} ms", System.currentTimeMillis() - startTime);
		return result;
	}

	// @Override
	@SneakyThrows
	public boolean fileExists(final Path path) {
		return fileSystem.exists(path);
	}

}
