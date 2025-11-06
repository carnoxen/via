package com.bitorgroup.via.smbs;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@SpringBootApplication
public class SmbsApplication {
	private SmbsClient client;

	public SmbsApplication(SmbsClient client) {
		this.client = client;
	}

	public static void main(String[] args) {
		SpringApplication.run(SmbsApplication.class, args);
	}

	@Bean
	public Supplier<Flux<String>> smbs() {
		return () -> Flux
				.<String>create(this.client)
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1)))
				.subscribeOn(Schedulers.boundedElastic())
				.share();
	}

}
