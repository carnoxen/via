package com.bitorgroup.via.trep;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@SpringBootApplication
@Slf4j
public class ViaTrepApplication {
    private TrepClient client;

    public ViaTrepApplication(TrepClient client) {
        this.client = client;
    }

    public static void main(String[] args) {
        SpringApplication.run(ViaTrepApplication.class, args);
    }

    @Bean
    @Scheduled(cron = "${start_at}")
    public Supplier<Flux<String>> execute() {
		return () -> Flux
				.<String>create(this.client)
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1)))
				.subscribeOn(Schedulers.boundedElastic())
				.share();
    }

}
