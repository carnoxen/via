package com.bitorgroup.via.smbs.handler;

import org.springframework.stereotype.Service;

import com.bitorgroup.via.smbs.SmbsClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SmbsWriteHandler implements SmbsHandler<Integer> {

    private SmbsReadHandler handler;

    public SmbsWriteHandler(SmbsReadHandler handler) {
        this.handler = handler;
    }

    @Override
    public void completed(Integer result, SmbsClient attachment) {
        var channel = attachment.getChannel();
        var buffer = attachment.getBuffer();

        buffer.clear();
        
        channel.read(buffer, attachment, this.handler);
    }

    @Override
    public void failed(Throwable exc, SmbsClient attachment) {
        log.error("ERROR EXECUTED IN WRITE: {}", exc);
        log.info("RESENDING...");
        var channel = attachment.getChannel();
        var buffer = attachment.getBuffer();

        buffer.flip();
        
        channel.write(buffer, attachment, this);
    }
    
}
