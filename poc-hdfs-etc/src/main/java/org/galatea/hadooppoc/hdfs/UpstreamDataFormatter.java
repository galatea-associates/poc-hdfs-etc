package org.galatea.hadooppoc.hdfs;

import java.io.File;
import java.nio.file.Files;

import org.galatea.hadooppoc.hdfs.jsonobjects.CounterParties;
import org.galatea.hadooppoc.hdfs.jsonobjects.Instruments;
import org.galatea.hadooppoc.hdfs.jsonobjects.JsonObject;
import org.galatea.hadooppoc.hdfs.jsonobjects.LegalEntities;
import org.galatea.hadooppoc.hdfs.jsonobjects.SwapHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class UpstreamDataFormatter {

	private ObjectMapper objectMapper;
	private DatanameExtractor datanameExtractor;

	public UpstreamDataFormatter() {
		objectMapper = new ObjectMapper();
		datanameExtractor = DatanameExtractor.newExtractor();
	}

	public JsonObject getFormattedData(final File file) {
		switch (datanameExtractor.extract(file)) {
		case "instruments":
			return getFormattedInstruments(file);
		case "legalEntity":
			return getFormattedLegalEntites(file);
		case "counterparty":
			return getFormattedCounterParties(file);
		case "swapHeader":
			return getFormattedSwapHeaders(file);
		default:
			return null;
		}
	}

	@SneakyThrows
	private Instruments getFormattedInstruments(final File file) {
		return objectMapper.readValue(Files.readAllBytes(file.toPath()), Instruments.class);
	}

	@SneakyThrows
	private LegalEntities getFormattedLegalEntites(final File file) {
		return objectMapper.readValue(Files.readAllBytes(file.toPath()), LegalEntities.class);
	}

	@SneakyThrows
	private CounterParties getFormattedCounterParties(final File file) {
		return objectMapper.readValue(Files.readAllBytes(file.toPath()), CounterParties.class);
	}

	@SneakyThrows
	private SwapHeaders getFormattedSwapHeaders(final File file) {
		return objectMapper.readValue(Files.readAllBytes(file.toPath()), SwapHeaders.class);
	}

}
