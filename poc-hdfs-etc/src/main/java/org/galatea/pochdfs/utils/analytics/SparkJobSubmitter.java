package org.galatea.pochdfs.utils.analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class SparkJobSubmitter {

	public void submit(final String[] submissionArgs, final SparkConf sparkConfiguration) throws Exception {
		// System.setProperty("SPARK_YARN_MODE", "true");
		ClientArguments clientArguments = new ClientArguments(submissionArgs);
		Client client = new Client(clientArguments, sparkConfiguration);
		client.run();
	}
}