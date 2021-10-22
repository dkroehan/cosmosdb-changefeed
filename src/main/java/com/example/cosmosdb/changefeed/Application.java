package com.example.cosmosdb.changefeed;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class Application implements CommandLineRunner {

	@Autowired
	private ChangeFeedRunnerFail changeFeedRunnerFail;

	@Autowired
	private ChangeFeedRunnerWorks changeFeedRunnerWorks;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		try {
			changeFeedRunnerFail.consumeChangeFeed();
		} catch (Exception e) {
			log.error("Failure", e);
		}

		try {
			changeFeedRunnerWorks.consumeChangeFeed();
		} catch (Exception e) {
			log.error("Failure", e);
		}
	}
}
