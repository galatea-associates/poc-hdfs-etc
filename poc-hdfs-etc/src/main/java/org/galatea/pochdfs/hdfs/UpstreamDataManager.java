package org.galatea.pochdfs.hdfs;

import java.io.File;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.galatea.pochdfs.hdfs.jsonobjects.CashFlow;
import org.galatea.pochdfs.hdfs.jsonobjects.CashFlows;
import org.galatea.pochdfs.hdfs.jsonobjects.JsonObject;
import org.galatea.pochdfs.hdfs.jsonobjects.Position;
import org.galatea.pochdfs.hdfs.jsonobjects.Positions;
import org.galatea.pochdfs.hdfs.jsonobjects.SwapHeader;
import org.galatea.pochdfs.hdfs.jsonobjects.SwapHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpstreamDataManager {

	private FileWriter writer;
	private UpstreamDataFormatter formatter;
	private FilepathConstructor filePathConstructor;
	private ObjectMapper objectMapper;

	private UpstreamDataManager(final FileWriter writer) {
		this.writer = writer;
		formatter = new UpstreamDataFormatter();
		objectMapper = new ObjectMapper();
		filePathConstructor = FilepathConstructor.newConstructor();
	}

	public static UpstreamDataManager newManager(final FileWriter writer) {
		return new UpstreamDataManager(writer);
	}

	public void writeData(final File file) {
		JsonObject jsonObject = formatter.getFormattedData(file);
		writeData(jsonObject);
	}

	public void writeData(final JsonObject jsonObject) {
		switch (jsonObject.getObjectType()) {
		case "counterParties":
			writeCounterPartyData(jsonObject);
			break;
		case "instruments":
			writeInstrumnetData(jsonObject);
			break;
		case "legalEntities":
			writeLegalEntityData(jsonObject);
			break;
		case "swapHeaders":
			writeSwapHeaderData(jsonObject);
			break;
		case "positions":
			writePositionsData(jsonObject);
			break;
		case "cashFlows":
			writeCashFlowsData(jsonObject);
		}
	}

	private void writeCounterPartyData(final JsonObject jsonObject) {
		Path filePath = new Path(filePathConstructor.constructCounterpartyFilename());
		writeFile(filePath, createByteArray(jsonObject.getData()));
	}

	private void writeInstrumnetData(final JsonObject jsonObject) {
		Path filePath = new Path(filePathConstructor.constructInstRefsFilename());
		writeFile(filePath, createByteArray(jsonObject.getData()));

	}

	private void writeLegalEntityData(final JsonObject jsonObject) {
		Path filePath = new Path(filePathConstructor.constructLegalEntityFilename());
		writeFile(filePath, createByteArray(jsonObject.getData()));
	}

	@SneakyThrows
	private byte[] createByteArray(final Collection<?> objects) {
		StringBuilder builder = new StringBuilder();
		for (Object object : objects) {
			builder.append(objectMapper.writeValueAsString(object)).append("\n");
		}
		return builder.toString().getBytes();
	}

	@SneakyThrows
	private void writeSwapHeaderData(final JsonObject jsonObject) {
		SwapHeaders swapHeaders = (SwapHeaders) jsonObject;
		for (SwapHeader swapHeader : swapHeaders.getData()) {
			Path filePath = new Path(filePathConstructor.constructSwapHeaderFilename((swapHeader).getCounterPartyId()));
			writeFile(filePath, createByteArray(swapHeader));
		}
	}

	@SneakyThrows
	private void writePositionsData(final JsonObject jsonObject) {
		Positions positions = (Positions) jsonObject;
		for (Position position : positions.getData()) {
			Path filePath = new Path(
					filePathConstructor.constructPositionFilename(position.getCounterPartyId(), position.getCobDate()));
			writeFile(filePath, createByteArray(position));
		}
	}

	@SneakyThrows
	private void writeCashFlowsData(final JsonObject jsonObject) {
		CashFlows cashFlows = (CashFlows) jsonObject;
		for (CashFlow cashFlow : cashFlows.getData()) {
			Path filePath = new Path(filePathConstructor.constructCashFlowFilename(cashFlow.getSwapId()));
			writeFile(filePath, createByteArray(cashFlow));
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
