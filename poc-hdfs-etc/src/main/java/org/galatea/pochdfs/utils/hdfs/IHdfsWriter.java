package org.galatea.pochdfs.utils.hdfs;

import org.apache.hadoop.fs.Path;

public interface IHdfsWriter {

	/**
	 * Creates a new JSONL file in HDFS at specified specific path
	 *
	 * @param path   the HDFS path of the file
	 * @param object the object to write in JSONL form to HDFS
	 */
	public void createFile(final Path path, final Object object);

	/**
	 * Appends JSONL to an existing file in HDFS at the specified path
	 *
	 * @param path   the HDFS path of the file
	 * @param object the object to append in JSONL form to HDFS
	 */
	public void appendFile(final Path path, final Object object);

	public void createFileFromByteArray(final Path path, final byte[] source);

	public void appendByteArrayToFile(final Path path, final byte[] source);

	/**
	 *
	 * @param path the HDFS path to the file
	 * @return true if the file exists
	 */
	public boolean fileExists(final Path path);

}
