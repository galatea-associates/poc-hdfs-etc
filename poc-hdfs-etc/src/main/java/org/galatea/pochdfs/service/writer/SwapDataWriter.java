package org.galatea.pochdfs.service.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.galatea.pochdfs.utils.hdfs.HdfsWriter;
import org.galatea.pochdfs.utils.hdfs.IHdfsFilePathGetter;
import org.galatea.pochdfs.utils.hdfs.JsonMapper;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * A writer to write swap data from upstream to HDFS
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class SwapDataWriter {

	private static final ObjectMapper	MAPPER	= new ObjectMapper();

	private final JsonMapper			objectMapper;

	private final SwapFilePathCreator	pathCreator;

	private final HdfsWriter			writer;

	@SneakyThrows
	public void writeSwapData(final File file) {
		String filename = file.getName();
		log.info("Writing {} data", filename);

		if (filename.toLowerCase().contains("counterparties")) {
			writeEntireFileToHdfs(file, pathCreator.createCounterpartyFilepath());
		} else if (filename.toLowerCase().contains("instruments")) {
			writeEntireFileToHdfs(file, pathCreator.createInstrumentsFilepath());
		} else if (filename.toLowerCase().contains("swapcontracts")) {
			writeSwapRecordsToHdfs(file, (jsonObject) -> {
				Long startTime = System.currentTimeMillis();
				String path = pathCreator.constructSwapContractFilepath((int) jsonObject.get("counterparty_id"));
				log.info("Created path in {} ms", System.currentTimeMillis() - startTime);
				return path;
			});
		} else if (filename.toLowerCase().contains("positions")) {
			writeSwapRecordsToHdfs(file, (jsonObject) -> {
				Long startTime = System.currentTimeMillis();
				String path = pathCreator.createPositionFilepath((int) jsonObject.get("swap_contract_id"));
				log.info("Created path in {} ms", System.currentTimeMillis() - startTime);
				return path;
			});
		} else if (filename.toLowerCase().contains("cashflows")) {
			writeSwapRecordsToHdfs(file, (jsonObject) -> {
				Long startTime = System.currentTimeMillis();
				String path = pathCreator.createCashFlowFilepath((int) jsonObject.get("swap_contract_id"));
				log.info("Created path in {} ms", System.currentTimeMillis() - startTime);
				return path;
			});
		} else {
			throw new FileNotFoundException("File " + filename + "not found");
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
		Map<String, StringBuilder> fileData = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			int recordCount = 0;
			String jsonLine = reader.readLine();
			while (jsonLine != null) {
				Long beginTime = System.currentTimeMillis();
				Map<String, Object> jsonObject = objectMapper.getTimestampedObject(jsonLine);
				log.info("Mapped object {} in {} ms", recordCount, System.currentTimeMillis() - beginTime);

				String filePath = pathGetter.getFilePath(jsonObject);
				StringBuilder builder = new StringBuilder(MAPPER.writeValueAsString(jsonObject));
				String recordData = builder.append("\n").toString();

				if (fileData.containsKey(filePath)) {
					fileData.get(filePath).append(recordData);
				} else {
					fileData.put(filePath, new StringBuilder(recordData));
				}

//				beginTime = System.currentTimeMillis();
//				writeObjectToHdfs(new Path(pathGetter.getFilePath(jsonObject)), jsonObject);
//				log.info("Wrote object {} to HDFS in {} ms", recordCount, System.currentTimeMillis() - beginTime);
				jsonLine = reader.readLine();
				recordCount++;
			}

			writeRecordsToHdfs(fileData);

			log.info("Processed {} records from file {} in {} ms", recordCount, file.getName(),
					System.currentTimeMillis() - startTime);
		}
	}

	private void writeRecordsToHdfs(final Map<String, StringBuilder> fileData) {
		// Long startTime = System.currentTimeMillis();

		fileData.forEach((filepath, builder) -> {
			writeBytesToHdfs(new Path(filepath), builder.toString().getBytes());
		});
	}

	private void writeEntireFileToHdfs(final File file, final String path) {
		// Long startTime = System.currentTimeMillis();
		writeFileToHdfs(new Path(path), file);
		// log.info("Processed entire file {} in {} ms", file.getName(),
		// System.currentTimeMillis() - startTime);
	}

	/**
	 *
	 * @param path   the HDFS path to write to
	 * @param object the object bring written to HDFS
	 */
//	@SneakyThrows
//	private void writeObjectToHdfs(final Path path, final Object object) {
//		if (writer.fileExists(path)) {
//			writer.appendFile(path, object);
//		} else {
//			writer.createFile(path, object);
//		}
//	}

	@SneakyThrows
	private void writeBytesToHdfs(final Path path, final byte[] data) {
		if (writer.fileExists(path)) {
			writer.appendByteArrayToFile(path, data);
		} else {
			writer.createFileFromByteArray(path, data);
		}
	}

	@SneakyThrows
	private void writeFileToHdfs(final Path path, final File file) {
		if (writer.fileExists(path)) {
			writer.appendByteArrayToFile(path, Files.readAllBytes(file.toPath()));
		} else {
			writer.createFileFromByteArray(path, Files.readAllBytes(file.toPath()));
		}
	}

}
