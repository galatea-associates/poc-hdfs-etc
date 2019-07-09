package org.galatea.pochdfs.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * A writer to write swap data from upstream to HDFS
 */
@Slf4j
@AllArgsConstructor
public class SwapDataWriter {

	private static final UpstreamObjectMapper OBJECT_MAPPER = UpstreamObjectMapper.getInstance();
	private static final FilePathCreator FILE_PATH_CREATOR = FilePathCreator.getInstance();

	private IHdfsWriter writer;

	public void writeSwapData(final File file) {
		log.info("Writing {} data", file.getName());
		switch (file.getName()) {
		case "counterparties.json":
			writeSwapRecordsToHdfs(file, (jsonObject) -> FILE_PATH_CREATOR.createCounterpartyFilepath());
			break;
		case "instruments.json":
			writeSwapRecordsToHdfs(file, (jsonObject) -> FILE_PATH_CREATOR.createInstrumentsFilepath());
			break;
		case "swapContracts.json":
			writeSwapRecordsToHdfs(file, (jsonObject) -> FILE_PATH_CREATOR
					.constructSwapContractFilepath((int) jsonObject.get("counterPartyId")));
			break;
		case "positions.json":
			writeSwapRecordsToHdfs(file, (jsonObject) -> FILE_PATH_CREATOR
					.createPositionFilepath((int) jsonObject.get("swapId"), (int) jsonObject.get("effectiveDate")));
			break;
		case "cashflows.json":
			writeSwapRecordsToHdfs(file,
					(jsonObject) -> FILE_PATH_CREATOR.createCashFlowFilepath((int) jsonObject.get("swapId")));
		}
	}

	/**
	 * Writes each individual record in JSON format to HDFS
	 *
	 * @param file the file to be written to HDFS
	 * @param path the path to write the records
	 */
	@SneakyThrows
	private void writeSwapRecordsToHdfs(final File file, final IHdfsFilePathGetter pathGetter) {
		Long startTime = System.currentTimeMillis();
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			int recordCount = 0;
			String jsonLine = reader.readLine();
			while (jsonLine != null) {
				Map<String, Object> jsonObject = OBJECT_MAPPER.getTimestampedObject(jsonLine);
				writeObjectToHdfs(new Path(pathGetter.getFilePath(jsonObject)), jsonObject);
				jsonLine = reader.readLine();
				recordCount++;
			}
			log.info("Processed {} records in {} ms", recordCount, System.currentTimeMillis() - startTime);
		}
	}

	/**
	 *
	 * @param path   the HDFS path to write to
	 * @param object the object bring written to HDFS
	 */
	@SneakyThrows
	private void writeObjectToHdfs(final Path path, final Object object) {
		if (writer.fileExists(path)) {
			writer.appendFile(path, object);
		} else {
			writer.createFile(path, object);
		}
	}

}
