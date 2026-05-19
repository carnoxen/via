package com.bitorgroup.via.smbs;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.bitorgroup.via.common.SmbsMessage;

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
	public Supplier<Flux<SmbsMessage>> smbs() {
		return () -> Flux
				.<SmbsMessage>create(this.client)
				.retry()
				.subscribeOn(Schedulers.boundedElastic())
				.share();
	}

}
