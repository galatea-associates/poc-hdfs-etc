package org.galatea.pochdfs.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SwapDataWriter {

	private FileWriter writer;
	private FilePathCreator filePathCreator;
	private UpstreamObjectMapper objectMapper;

	public SwapDataWriter(final FileWriter writer) {
		this.writer = writer;
		filePathCreator = FilePathCreator.getInstance();
		objectMapper = new UpstreamObjectMapper();
	}

	public void writeData(final File file) {
		log.info("Writing {} data", file.getName());
		switch (file.getName()) {
		case "counterParties.json":
			writeFile(file, (jsonObject) -> filePathCreator.createCounterpartyFilepath());
			break;
		case "instruments.json":
			writeFile(file, (jsonObject) -> filePathCreator.createInstrumentsFilepath());
			break;
		case "swapContracts.json":
			writeFile(file, (jsonObject) -> filePathCreator
					.constructSwapContractFilepath((int) jsonObject.get("counterPartyId")));
			break;
		case "positions.json":
			writeFile(file, (jsonObject) -> filePathCreator.createPositionFilepath((int) jsonObject.get("swapId"),
					(int) jsonObject.get("effectiveDate")));
			break;
		case "cashFlows.json":
			writeFile(file, (jsonObject) -> filePathCreator.createCashFlowFilepath((int) jsonObject.get("swapId")));
		}
	}

	@SneakyThrows
	private void writeFile(final File file, final IFilePath path) {
		Long startTime = System.currentTimeMillis();
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			int recordCount = 0;
			String jsonLine = reader.readLine();
			while (jsonLine != null) {
				Map<String, Object> jsonObject = objectMapper.getTimestampedObject(jsonLine);
				writeToHdfs(new Path(path.getFilePath(jsonObject)), jsonObject);
				jsonLine = reader.readLine();
				recordCount++;
			}
			log.info("Processed {} records in {} ms", recordCount, System.currentTimeMillis() - startTime);
		}
	}

	@SneakyThrows
	private void writeToHdfs(final Path path, final Object object) {
		if (writer.getFileSystem().exists(path)) {
			writer.appendFile(path, object);
		} else {
			writer.createFile(path, object);
		}
	}

}
