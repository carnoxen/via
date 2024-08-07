package com.bitor_group.via;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
public class ViaStreamInTrepApplication {
	private TrepClient trepClient;

	public ViaStreamInTrepApplication(TrepClient trepClient) {
		this.trepClient = trepClient;
	}

	public static void main(String[] args) {
		SpringApplication.run(ViaStreamInTrepApplication.class, args);
	}

	@Bean
	@Scheduled(cron = "")
	public void execute() {
		this.trepClient.start();
	}

	@Bean
	@Scheduled(cron = "")
	public void close() {
		this.trepClient.close();
	}

}
