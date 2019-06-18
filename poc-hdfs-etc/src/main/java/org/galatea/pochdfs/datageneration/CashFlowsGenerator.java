package org.galatea.pochdfs.datageneration;

import java.util.ArrayList;
import java.util.Random;

import org.galatea.pochdfs.hdfs.jsonobjects.CashFlows;
import org.galatea.pochdfs.hdfs.jsonobjects.CounterParty;

public class CashFlowsGenerator {

	private Random random;

	public CashFlowsGenerator() {
		random = new Random();
	}

	public CashFlows generateCashFlows(final int numberOfCashFlows) {
		CashFlows cashFlows = new CashFlows(new ArrayList<>());
		for (int i = 1; i <= numberOfCashFlows; i++) {
			log.info("Generating counterParty number {}", i);
			cashFlows.addCashFlow(new CounterParty(getRandomCounterPartyId(), getRandomEntity(), getRandomCode()));
		}
		return counterParties;
	}

}
