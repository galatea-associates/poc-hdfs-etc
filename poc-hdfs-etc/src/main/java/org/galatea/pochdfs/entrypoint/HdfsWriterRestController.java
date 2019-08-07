package org.galatea.pochdfs.entrypoint;

import org.galatea.pochdfs.service.writer.SwapDataWriterService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
//@RestController
public class HdfsWriterRestController {

	private final SwapDataWriterService writer;

	@PostMapping(value = "/write", produces = { MediaType.ALL_VALUE })
	public ResponseEntity<String> writeEndpoint(
			@RequestParam(value = "filePath", required = true) final String filePath) {
		try {
			writer.writeData(filePath);
			return new ResponseEntity<>("Write Swap Data to Local Succeeded", HttpStatus.OK);
		} catch (Exception e) {
			return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

}
