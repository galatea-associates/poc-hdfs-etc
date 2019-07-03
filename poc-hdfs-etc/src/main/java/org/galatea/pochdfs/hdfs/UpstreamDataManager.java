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
			writeCounterPartyData(file);
			break;
		case "instruments.json":
			writeInstrumentData(file);
			break;
		case "swapContracts.json":
			writeSwapContractData(file);
			break;
		case "positions.json":
			writePositionsData(file);
			break;
		case "cashFlows.json":
			writeCashFlowsData(file);
		}
	}

	@SneakyThrows
	private void writeCounterPartyData(final File file) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String counterPartyObjectLine = reader.readLine();
			while (counterPartyObjectLine != null) {
				Map<String, Object> counterParty = objectMapper.readValue(counterPartyObjectLine.getBytes(),
						JSON_OBJECT_TYPE_REFERENCE);
				counterParty.put("timeStamp", Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString()));
				Path filePath = new Path(filePathConstructor.constructCounterpartyFilename());
				writeFile(filePath, createByteArray(counterParty));
				counterPartyObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeInstrumentData(final File file) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String instrumentObjectLine = reader.readLine();
			while (instrumentObjectLine != null) {
				Map<String, Object> instrument = objectMapper.readValue(instrumentObjectLine.getBytes(),
						JSON_OBJECT_TYPE_REFERENCE);
				instrument.put("timeStamp", Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString()));
				Path filePath = new Path(filePathConstructor.constructInstRefsFilename());
				writeFile(filePath, createByteArray(instrument));
				instrumentObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeSwapContractData(final File file) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String swapContractObjectLine = reader.readLine();
			while (swapContractObjectLine != null) {
				Map<String, Object> swapContract = objectMapper.readValue(swapContractObjectLine.getBytes(),
						JSON_OBJECT_TYPE_REFERENCE);
				swapContract.put("timeStamp", Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString()));
				Path filePath = new Path(
						filePathConstructor.constructSwapContractFilename((int) swapContract.get("counterPartyId")));
				writeFile(filePath, createByteArray(swapContract));
				swapContractObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writePositionsData(final File file) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String positionObjectLine = reader.readLine();
			while (positionObjectLine != null) {
				Map<String, Object> position = objectMapper.readValue(positionObjectLine.getBytes(),
						JSON_OBJECT_TYPE_REFERENCE);
				position.put("timeStamp", Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString()));
				Path filePath = new Path(filePathConstructor.constructPositionFilename((int) position.get("swapId"),
						(int) position.get("effectiveDate")));
				writeFile(filePath, createByteArray(position));
				positionObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeCashFlowsData(final File file) {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String cashFlowObjectLine = reader.readLine();
			while (cashFlowObjectLine != null) {
				Map<String, Object> cashFlow = objectMapper.readValue(cashFlowObjectLine.getBytes(),
						JSON_OBJECT_TYPE_REFERENCE);
				cashFlow.put("timeStamp", Integer.valueOf(LocalDateTime.now().format(FORMATTER).toString()));
				Path filePath = new Path(filePathConstructor.constructCashFlowFilename((int) cashFlow.get("swapId")));
				writeFile(filePath, createByteArray(cashFlow));
				cashFlowObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private byte[] createByteArray(final Object object) {
		StringBuilder builder = new StringBuilder(objectMapper.writeValueAsString(object));
		return builder.append("\n").toString().getBytes();
	}

	@SneakyThrows
	private void writeFile(final Path path, final byte[] source) {
		if (writer.getFileSystem().exists(path)) {
			writer.appendFile(path, source);
		} else {
			writer.createFile(path, source);
		}
	}

}
