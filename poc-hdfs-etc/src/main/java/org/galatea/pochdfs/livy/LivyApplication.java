package org.galatea.pochdfs.livy;

public class LivyApplication {

//	@SneakyThrows
//	public static void main(final String[] args) {
//		int samples = 2;
//
//		URI uri = new URI("http://ec2-18-222-172-50.us-east-2.compute.amazonaws.com:8998");
//
//		Map<String, String> config = new HashMap<>();
//		config.put("spark.app.name", "livy-poc");
//		config.put("livy.client.http.connection.timeout", "180s");
//		config.put("spark.driver.memory", "400m");
//		config.put("livy.spark.master", "yarn");
//
//		LivyClient client = new LivyClientBuilder(false).setURI(uri).setAll(config).build();
//
//		try {
//			// System.err.printf("Uploading %s to the Spark context...\n", piJar);
//
////			for (String s : System.getProperty("java.class.path").split(File.pathSeparator)) {
////				if (new File(s).getName().startsWith("snapshot")) {
////					client.uploadJar(new File(s)).get();
////					break;
////				}
////			}
//
//			client.uploadJar(new File(
//					"C:\\Users\\kpayne\\git\\poc-hdfs-etc\\poc-hdfs-etc\\target\\hadoop-poc-0.0.1-SNAPSHOT.jar")).get();
//
//			System.out.printf("Running PiJob with %d samples...\n", samples);
//			Dataset<Row> result = client.submit(new EnrichPositionsAndUnpaidCashJob()).get();
//
//			System.out.println(result.toString());
//		} finally {
//			client.stop(true);
//		}
//	}
}
