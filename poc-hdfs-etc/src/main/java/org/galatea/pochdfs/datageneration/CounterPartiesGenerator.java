package org.galatea.pochdfs.datageneration;

import java.util.ArrayList;
import java.util.Random;

import org.galatea.pochdfs.hdfs.jsonobjects.CounterParties;
import org.galatea.pochdfs.hdfs.jsonobjects.CounterParty;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CounterPartiesGenerator {

	private static final int COUNTER_PARTY_ID_LIMIT = 500;

	private static final String[] COUNTER_PARTY_ENTITIES = new String[] { "300", "301", "302", "BDL" };

	private static final String[] COUNTER_PARTY_CODE = new String[] { "YULND0", "YC1610", "CSFBOS1LONGB" };

	private Random random;

	public CounterPartiesGenerator() {
		random = new Random();
	}

	public CounterParties generateCounterParties(final int numberOfCounterParties) {
		CounterParties counterParties = new CounterParties(new ArrayList<>());
		for (int i = 1; i <= numberOfCounterParties; i++) {
			log.info("Generating counterParty number {}", i);
			counterParties
					.addCounterParty(new CounterParty(getRandomCounterPartyId(), getRandomEntity(), getRandomCode()));
		}
		return counterParties;
	}

	private String getRandomCounterPartyId() {
		return String.valueOf(random.nextInt(COUNTER_PARTY_ID_LIMIT + 1));
	}

	private String getRandomEntity() {
		return COUNTER_PARTY_ENTITIES[random.nextInt(COUNTER_PARTY_ENTITIES.length)];
	}

	private String getRandomCode() {
		return COUNTER_PARTY_ENTITIES[random.nextInt(COUNTER_PARTY_CODE.length)];
	}

}
