package org.galatea.pochdfs.hdfs;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class UpstreamObjectMapper {

	// private static final UpstreamObjectMapper INSTANCE = new
	// UpstreamObjectMapper();
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddss");
	private static final TypeReference<HashMap<String, Object>> TYPE_REFERENCE = new TypeReference<HashMap<String, Object>>() {
	};

	private ObjectMapper objectMapper;

	public UpstreamObjectMapper() {
		objectMapper = new ObjectMapper();
	}

//	public UpstreamObjectMapper getInstance() {
//		return INSTANCE;
//	}

	@SneakyThrows
	public Map<String, Object> getTimestampedObject(final String jsonObject) {
		Map<String, Object> object = objectMapper.readValue(jsonObject.getBytes(), TYPE_REFERENCE);
		if (object.containsKey("timeStamp")) {
			object.replace("timeStamp", getCurrentTime());
		} else {
			object.put("timeStamp", getCurrentTime());
		}
		return object;
	}

	private int getCurrentTime() {
		return Integer.valueOf(LocalDateTime.now().format(DATE_TIME_FORMATTER).toString());
	}

}
