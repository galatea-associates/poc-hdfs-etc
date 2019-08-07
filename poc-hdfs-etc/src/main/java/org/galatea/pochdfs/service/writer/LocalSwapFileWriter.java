package org.galatea.pochdfs.service.writer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.galatea.pochdfs.utils.hdfs.IHdfsFilePathGetter;
import org.galatea.pochdfs.utils.hdfs.JsonMapper;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class LocalSwapFileWriter {

	private static final ObjectMapper	MAPPER	= new ObjectMapper();

	private final SwapFilePathCreator	pathCreator;

	private final JsonMapper			objectMapper;

	@SneakyThrows
	public void writeSwapData(final String localFilePath, final String targetBasePath) {
		File file = new File(localFilePath);
		String filename = file.getName();
		log.info("Writing {} data", filename);

		if (filename.toLowerCase().contains("counterparties")) {
			writeStringToFile(new String(Files.readAllBytes(file.toPath())), pathCreator.createCounterpartyFilepath(),
					targetBasePath);
		} else if (filename.toLowerCase().contains("instruments")) {
			writeStringToFile(new String(Files.readAllBytes(file.toPath())), pathCreator.createInstrumentsFilepath(),
					targetBasePath);
		} else if (filename.toLowerCase().contains("swapcontracts")) {
			writeRecords(file, targetBasePath, (jsonObject) -> {
				return pathCreator.constructSwapContractFilepath((int) jsonObject.get("counterparty_id"));
			});
		} else if (filename.toLowerCase().contains("positions")) {
			writeRecords(file, targetBasePath, (jsonObject) -> {
				return pathCreator.createPositionFilepath((int) jsonObject.get("swap_contract_id"));
			});
		} else if (filename.toLowerCase().contains("cashflows")) {
			writeRecords(file, targetBasePath, (jsonObject) -> {
				return pathCreator.createCashFlowFilepath((int) jsonObject.get("swap_contract_id"));
			});
		} else {
			throw new FileNotFoundException("File " + filename + "not found");
		}
	}

	@SneakyThrows
	private void writeRecords(final File file, final String targetBasePath, final IHdfsFilePathGetter pathGetter) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			int recordCount = 0;
			String jsonLine = reader.readLine();
			while (jsonLine != null) {
				Map<String, Object> jsonObject = objectMapper.getTimestampedObject(jsonLine);

				String filePath = pathGetter.getFilePath(jsonObject);
				StringBuilder builder = new StringBuilder(MAPPER.writeValueAsString(jsonObject));
				String recordData = builder.append("\n").toString();

				writeStringToFile(recordData, filePath, targetBasePath);
				log.info("Wrote object {} to file", recordCount);
				jsonLine = reader.readLine();
				recordCount++;
			}
		}
	}

	@SneakyThrows
	private void writeStringToFile(final String data, final String filePath, final String targetBasePath) {
		log.info(targetBasePath);
		log.info(filePath);
		Files.createDirectories(Paths.get(targetBasePath + filePath).getParent());
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(targetBasePath + filePath, true))) {
			writer.write(data);

		}
	}

}
