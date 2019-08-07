package org.galatea.pochdfs.service.writer;

import java.io.File;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Component
public class SwapDataWriterService {

	private final SwapDataWriter writer;

	public void writeData(final String filePath) {
		writer.writeSwapData(new File(filePath));
	}

}
