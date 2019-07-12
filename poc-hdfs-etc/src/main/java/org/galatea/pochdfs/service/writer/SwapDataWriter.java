package org.galatea.pochdfs.service.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.galatea.pochdfs.utils.hdfs.IHdfsFilePathGetter;
import org.galatea.pochdfs.utils.hdfs.IHdfsWriter;
import org.galatea.pochdfs.utils.hdfs.JsonMapper;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * A writer to write swap data from upstream to HDFS
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class SwapDataWriter {

	private static final JsonMapper OBJECT_MAPPER = JsonMapper.getInstance();
	private static final SwapFilePathCreator FILE_PATH_CREATOR = SwapFilePathCreator.getInstance();

	private final IHdfsWriter writer;

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
					.constructSwapContractFilepath((int) jsonObject.get("counterparty_id")));
			break;
		case "positions.json":
			writeSwapRecordsToHdfs(file,
					(jsonObject) -> FILE_PATH_CREATOR.createPositionFilepath((int) jsonObject.get("swap_contract_id"),
							(int) jsonObject.get("effective_date")));
			break;
		case "cashflows.json":
			writeSwapRecordsToHdfs(file,
					(jsonObject) -> FILE_PATH_CREATOR.createCashFlowFilepath((int) jsonObject.get("swap_contract_id")));
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
