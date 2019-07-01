package org.galatea.pochdfs.hdfs;


public class UpstreamDataFormatter {


	public UpstreamDataFormatter() {

	}

//	public JsonObject getFormattedData(final File file) {
//		switch (datanameExtractor.extract(file)) {
//		case "instruments":
//			return getFormattedInstruments(file);
//		case "legalEntity":
//			return getFormattedLegalEntites(file);
//		case "counterparty":
//			return getFormattedCounterParties(file);
//		case "swapHeader":
//			return getFormattedSwapHeaders(file);
//		case "positions":
//			return getFormattedPositions(file);
//		case "cashFlows":
//			return getFormattedCashFlows(file);
//		default:
//			return null;
//		}
//	}
//
//	@SneakyThrows
//	private Instruments getFormattedInstruments(final File file) {
//		return objectMapper.readValue(Files.readAllBytes(file.toPath()), Instruments.class);
//	}
//
//	@SneakyThrows
//	private LegalEntities getFormattedLegalEntites(final File file) {
//		return objectMapper.readValue(Files.readAllBytes(file.toPath()), LegalEntities.class);
//	}
//
//	@SneakyThrows
//	private CounterParties getFormattedCounterParties(final File file) {
//		return objectMapper.readValue(Files.readAllBytes(file.toPath()), CounterParties.class);
//	}
//
//	@SneakyThrows
//	private SwapHeaders getFormattedSwapHeaders(final File file) {
//		return objectMapper.readValue(Files.readAllBytes(file.toPath()), SwapHeaders.class);
//	}
//
//	@SneakyThrows
//	private Positions getFormattedPositions(final File file) {
//		return objectMapper.readValue(Files.readAllBytes(file.toPath()), Positions.class);
//	}
//
//	@SneakyThrows
//	public CashFlows getFormattedCashFlows(final File file) {
//		CashFlows cashFlows = new CashFlows();
//		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
//			String jsonObjectLine = reader.readLine();
//			while(jsonObjectLine != null) {
//				cashFlows.addCashFlow(objectMapper.readValue(jsonObjectLine.getBytes(), CashFlow.class));
//			}
//		}
//		return cashFlows;
//	}

}
