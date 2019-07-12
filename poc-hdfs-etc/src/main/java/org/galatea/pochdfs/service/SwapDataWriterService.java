package org.galatea.pochdfs.service;

import java.io.File;

import org.galatea.pochdfs.service.writer.SwapDataWriter;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class SwapDataWriterService {

	private final SwapDataWriter writer;

	public void writeData() {
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\cashflows.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\positions.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\counterparties.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\instruments.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\swapContracts.json"));
	}

}
