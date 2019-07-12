package org.galatea.pochdfs;

import org.apache.commons.cli.MissingOptionException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class Application implements ApplicationRunner {

	@SneakyThrows
	public static void main(final String[] args) {
		for (String arg : args) {
			log.info("Command Line Arg is {}", arg);
		}
		SpringApplication.run(Application.class, args);
	}

	/**
	 * Ensure that server port is passed in as a command line argument.
	 *
	 * @param args command line arguments
	 * @throws MissingOptionException if server port not provided as argument
	 */
	@Override
	@SneakyThrows
	public void run(final ApplicationArguments args) {
		if (!args.containsOption("server.port") && System.getProperty("server.port") == null) {
			throw new MissingOptionException("Server port must be set via command line parameter");
		}
	}

}
