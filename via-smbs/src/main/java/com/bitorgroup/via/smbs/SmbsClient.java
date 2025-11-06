package com.bitorgroup.via.smbs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.function.Consumer;

import org.springframework.stereotype.Component;

import com.bitorgroup.via.common.SmbsMessage;
import com.bitorgroup.via.common.ViaClient;
import com.bitorgroup.via.smbs.handler.*;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.FluxSink;

@Data
@Slf4j
@Component
public class SmbsClient implements ViaClient<SmbsMessage> {
    SmbsConfiguration configuration;
    SmbsConnectHandler handler;
    SmbsSequenceRepository repository;
    
    AsynchronousSocketChannel channel;
    ByteBuffer buffer = ByteBuffer.allocate(2048);
    FluxSink<SmbsMessage> emitter;

    public SmbsClient(
        SmbsConfiguration configuration, 
        SmbsSequenceRepository repository,
        SmbsConnectHandler handler) {
        this.configuration = configuration;
        this.repository = repository;
        this.handler = handler;
    }

    @Override
    public void accept(FluxSink<SmbsMessage> emitter) {
        this.emitter = emitter;
        var uri = URI.create(this.configuration.getHost());
        var hostname = uri.getHost();
        var port = uri.getPort();
        var address = new InetSocketAddress(hostname, port);
        var record = this.configuration.getRecord();

        var sequence = this.repository.findById(record);
        if (sequence.isEmpty()) {
            log.info("THIS IS THE FIRST REQUEST ON DAY.");
            var newSequence = SmbsSequence.builder().name(record).build();
            this.repository.save(newSequence);
        }

        log.info("OK. START CLIENT.");
        try {
            this.channel = AsynchronousSocketChannel.open();
            channel.connect(address, this, this.handler);
        } catch (IOException e) {
            log.error("ERROR ON START: {}", e);
            this.emitter.error(e);
        }
    }
}
