package org.galatea.pochdfs.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.fs.Path;
import org.galatea.pochdfs.hdfs.jsonobjects.CashFlow;
import org.galatea.pochdfs.hdfs.jsonobjects.CounterParty;
import org.galatea.pochdfs.hdfs.jsonobjects.Instrument;
import org.galatea.pochdfs.hdfs.jsonobjects.LegalEntity;
import org.galatea.pochdfs.hdfs.jsonobjects.Position;
import org.galatea.pochdfs.hdfs.jsonobjects.SwapHeader;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpstreamDataManager {

	private FileWriter writer;
	private FilepathConstructor filePathConstructor;
	private ObjectMapper objectMapper;

	private UpstreamDataManager(final FileWriter writer) {
		this.writer = writer;
		objectMapper = new ObjectMapper();
		filePathConstructor = FilepathConstructor.newConstructor();
	}

	public static UpstreamDataManager newManager(final FileWriter writer) {
		return new UpstreamDataManager(writer);
	}

	public void writeData(final File file) {
		log.info("Writing {} data", file.getName());
		switch (file.getName()) {
		case "counterParties":
			writeCounterPartyData(file);
			break;
		case "instruments":
			writeInstrumentData(file);
			break;
		case "legalEntities":
			writeLegalEntityData(file);
			break;
		case "swapHeaders":
			writeSwapHeaderData(file);
			break;
		case "positions":
			writePositionsData(file);
			break;
		case "cashFlows":
			writeCashFlowsData(file);
		}
	}
	
	@SneakyThrows
	private void writeCounterPartyData(final File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String counterPartyObjectLine = reader.readLine();
			while(counterPartyObjectLine != null) {
				CounterParty counterParty = objectMapper.readValue(counterPartyObjectLine.getBytes(), CounterParty.class);
				Path filePath = new Path(filePathConstructor.constructCounterpartyFilename());
				writeFile(filePath, createByteArray(counterParty));
				counterPartyObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeInstrumentData(final File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String instrumentObjectLine = reader.readLine();
			while(instrumentObjectLine != null) {
				Instrument instrument = objectMapper.readValue(instrumentObjectLine.getBytes(), Instrument.class);
				Path filePath = new Path(filePathConstructor.constructInstRefsFilename());
				writeFile(filePath, createByteArray(instrument));
				instrumentObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeLegalEntityData(final File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String legalEntityObjectLine = reader.readLine();
			while(legalEntityObjectLine != null) {
				LegalEntity legalEntity = objectMapper.readValue(legalEntityObjectLine.getBytes(), LegalEntity.class);
				Path filePath = new Path(filePathConstructor.constructLegalEntityFilename());
				writeFile(filePath, createByteArray(legalEntity));
				legalEntityObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeSwapHeaderData(final File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String swapHeaderObjectLine = reader.readLine();
			while(swapHeaderObjectLine != null) {
				SwapHeader swapHeader = objectMapper.readValue(swapHeaderObjectLine.getBytes(), SwapHeader.class);
				Path filePath = new Path(filePathConstructor.constructSwapHeaderFilename(swapHeader.getCounterPartyId()));
				writeFile(filePath, createByteArray(swapHeader));
				swapHeaderObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writePositionsData(final File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String positionObjectLine = reader.readLine();
			while(positionObjectLine != null) {
				Position position = objectMapper.readValue(positionObjectLine.getBytes(), Position.class);
				Path filePath = new Path(filePathConstructor.constructPositionFilename(position.getCounterPartyId(), position.getCobDate()));
				writeFile(filePath, createByteArray(position));
				positionObjectLine = reader.readLine();
			}
		}
	}

	@SneakyThrows
	private void writeCashFlowsData(final File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String cashFlowObjectLine = reader.readLine();
			while(cashFlowObjectLine != null) {
				CashFlow cashFlow = objectMapper.readValue(cashFlowObjectLine.getBytes(), CashFlow.class);
				Path filePath = new Path(filePathConstructor.constructCashFlowFilename(cashFlow.getSwapId()));
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
