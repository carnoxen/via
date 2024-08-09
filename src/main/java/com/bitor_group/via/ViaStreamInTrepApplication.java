package com.bitor_group.via;

import java.util.stream.Stream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@SpringBootApplication
@Slf4j
public class ViaStreamInTrepApplication {
    private TrepClient trepClient;
    private BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    public ViaStreamInTrepApplication(TrepClient trepClient) {
        this.trepClient = trepClient;
    }

    public static void main(String[] args) {
        SpringApplication.run(ViaStreamInTrepApplication.class, args);
    }

    @Bean
    @Scheduled(cron = "${start_at}")
    public Supplier<Flux<String>> execute() {
        this.trepClient.start(queue);
        return () -> Flux.fromStream(Stream.generate(() -> {
            String result = "";
            try {
                result = queue.take();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }))
        .subscribeOn(Schedulers.boundedElastic()).share();
    }

}
