package org.galatea.pochdfs.test.enrichedpositions;

import org.galatea.pochdfs.domain.Defaults;
import org.galatea.pochdfs.domain.input.Contract;
import org.galatea.pochdfs.domain.input.CounterParty;
import org.galatea.pochdfs.domain.input.Instrument;
import org.galatea.pochdfs.domain.input.Position;
import org.galatea.pochdfs.domain.result.EnrichedPositionResult;
import org.galatea.pochdfs.domain.result.EnrichedPositionsResults;
import org.galatea.pochdfs.util.SwapQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicTest extends SwapQueryTest {

	private static final long	serialVersionUID	= 1L;
	private static final int	S_TD_QUANTITY		= 100;

	@BeforeClass
	public static void writeData() {
		Contract.defaultContract().write();
		CounterParty.defaultCounterParty().write();
		Instrument.defaultInstrument().write();
		Position.defaultPosition().position_type("S").td_quantity(S_TD_QUANTITY).write();
		Position.defaultPosition().position_type("E").td_quantity(50).write();
		Position.defaultPosition().position_type("I").td_quantity(75).write();
	}

	@Test
	public void testEnrichedPositionsQuery() {
		EnrichedPositionResult result = resultGetter.getSingleEnrichedPositionResult(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE);

		result.assertCounterPartyIdEquals(Defaults.COUNTERPARTY_ID);
		result.assertEffectiveDateEquals(Defaults.EFFECTIVE_DATE);
		result.assertInstIdEquals(Defaults.INSTRUMENT_ID);
		result.assertCounterPartyField1Equals(Defaults.COUNTERPARTY_FIELD1);
		result.assertRicEquals(Defaults.RIC);
		result.assertSwapIdEquals(Defaults.CONTRACT_ID);
		result.assertTdQuantityEquals(S_TD_QUANTITY);
		result.assertBookEquals(Defaults.BOOK);
	}

	@Test
	public void testNoEffectiveDatePositions() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults(Defaults.BOOK,
				Defaults.EFFECTIVE_DATE + 1);
		results.assertResultCountEquals(0);
	}

	@Test
	public void testNoCounterParty() {
		EnrichedPositionsResults results = resultGetter.getEnrichedPositionResults("missingBook",
				Defaults.EFFECTIVE_DATE);
		results.assertResultCountEquals(0);
	}

}
