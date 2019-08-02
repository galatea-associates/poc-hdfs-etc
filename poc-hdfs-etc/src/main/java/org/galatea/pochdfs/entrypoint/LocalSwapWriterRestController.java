package org.galatea.pochdfs.entrypoint;

import org.galatea.pochdfs.LocalSwapFileWriter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@RestController
public class LocalSwapWriterRestController {

	private final LocalSwapFileWriter writer;

	@PostMapping(value = "/writelocal", produces = { MediaType.ALL_VALUE })
	public ResponseEntity<String> writeEndpoint(
			@RequestParam(value = "filePath", required = true) final String sourceFilePath,
			@RequestParam(value = "targetBasePath", required = true) final String targetBasePath) {
		try {
			writer.writeSwapData(sourceFilePath, targetBasePath);
			return new ResponseEntity<>("Write Swap Data to HDFS Succeeded", HttpStatus.OK);
		} catch (Exception e) {
			return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

}
