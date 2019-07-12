package org.galatea.pochdfs.utils.hdfs;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class JsonMapper {

	private static final JsonMapper INSTANCE = new JsonMapper();
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddss");
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final TypeReference<HashMap<String, Object>> TYPE_REFERENCE = new TypeReference<HashMap<String, Object>>() {
	};

	private JsonMapper() {
	}

	/**
	 *
	 * @return the instance of UpstreamObjectMapper
	 */
	public static JsonMapper getInstance() {
		return INSTANCE;
	}

	/**
	 *
	 * @param jsonObject the JSON string to map to an object
	 * @return the mapped JSON with a "timeStamp" field
	 */
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
