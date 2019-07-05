package org.galatea.pochdfs.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpstreamDataManager {

	TypeReference<HashMap<String, Object>> JSON_OBJECT_TYPE_REFERENCE = new TypeReference<HashMap<String, Object>>() {
	};
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddss");

	private FileWriter writer;
	private FilepathConstructor filePathConstructor;
	private ObjectMapper objectMapper;

	private UpstreamDataManager(final FileWriter writer) {
		this.writer = writer;
		objectMapper = new ObjectMapper();
		filePathConstructor = FilepathConstructor.getInstance();
	}

	public static UpstreamDataManager newManager(final FileWriter writer) {
		return new UpstreamDataManager(writer);
	}

	public void writeData(final File file) {
		log.info("Writing {} data", file.getName());
		switch (file.getName()) {
		case "counterParties.json":
			writeData(file, (jsonObject) -> filePathConstructor.constructCounterpartyFilepath());
			break;
		case "instruments.json":
			writeData(file, (jsonObject) -> filePathConstructor.constructInstRefsFilepath());
			break;
		case "swapContracts.json":
			writeData(file, (jsonObject) -> filePathConstructor
					.constructSwapContractFilepath((int) jsonObject.get("counterPartyId")));
			break;
		case "positions.json":
			writeData(file, (jsonObject) -> filePathConstructor
					.constructPositionFilepath((int) jsonObject.get("swapId"), (int) jsonObject.get("effectiveDate")));
			break;
		case "cashFlows.json":
			writeData(file,
					(jsonObject) -> filePathConstructor.constructCashFlowFilepath((int) jsonObject.get("swapId")));
		}
	}

	@SneakyThrows
	private void writeData(final File file, final IFilePath path) {
		Long startTime = System.currentTimeMillis();
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			int recordCount = 0;
			String jsonLine = reader.readLine();
			while (jsonLine != null) {
				Map<String, Object> jsonObject = objectMapper.readValue(jsonLine.getBytes(),
						JSON_OBJECT_TYPE_REFERENCE);
				Path filePath = new Path(path.getFilePath(jsonObject));
				jsonObject.put("timeStamp", getCurrentTime());
				writeBytesToHdfs(filePath, createByteArray(jsonObject));
				jsonLine = reader.readLine();
				recordCount++;
			}
			log.info("Processed {} records in {} ms", recordCount, System.currentTimeMillis() - startTime);
		}
	}

	private int getCurrentTime() {
		return Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString());
	}

	@SneakyThrows
	private byte[] createByteArray(final Object object) {
		StringBuilder builder = new StringBuilder(objectMapper.writeValueAsString(object));
		return builder.append("\n").toString().getBytes();
	}

	@SneakyThrows
	private void writeBytesToHdfs(final Path path, final byte[] source) {
		if (writer.getFileSystem().exists(path)) {
			writer.appendFile(path, source);
		} else {
			writer.createFile(path, source);
		}
	}

}
