package org.galatea.pochdfs.hdfs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.SneakyThrows;

/**
 * A file writer to write to HDFS
 */
@Getter
//@AllArgsConstructor
public class FileWriter {

	private FileSystem fileSystem;
	private ObjectMapper mapper;

	public FileWriter(final FileSystem fileSystem) {
		this.fileSystem = fileSystem;
		mapper = new ObjectMapper();
	}

	@SneakyThrows
	public void createFile(final Path path, final Object object) {
		createFile(path, createByteArray(object));
	}

	@SneakyThrows
	public void appendFile(final Path path, final Object object) {
		appendFile(path, createByteArray(object));
	}

	/**
	 * Creates a new file in HDFS
	 *
	 * @param path   the HDFS path of the new file
	 * @param source the byte array of the file to be created in HDFS
	 */
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

	/**
	 * Appends to an existing file in HDFS
	 *
	 * @param path   the HDFS path of the file to append to
	 * @param source the byte array of the file to be created in HDFS
	 */
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

	@SneakyThrows
	private byte[] createByteArray(final Object object) {
		StringBuilder builder = new StringBuilder(mapper.writeValueAsString(object));
		return builder.append("\n").toString().getBytes();
	}

}
