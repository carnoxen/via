package com.bitorgroup.via.smbs.handler;

import org.springframework.stereotype.Service;

import com.bitorgroup.via.smbs.SmbsClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SmbsConnectHandler implements SmbsHandler<Void> {

    private SmbsWriteHandler handler;

    public SmbsConnectHandler(SmbsWriteHandler handler) {
        this.handler = handler;
    }

    @Override
    public void completed(Void result, SmbsClient attachment) {
        var channel = attachment.getChannel();
        var buffer = attachment.getBuffer();
        var configuration = attachment.getConfiguration();
        var repository = attachment.getRepository();

        log.info("CONNECTED SUCCESSFULLY. SAY HELLO TO SMBS.");
        
        var record = configuration.getRecord();
        var message = configuration.hello();
        if (repository.existsById(record)) {
            var sequence = repository.findById(record).get();
            message = configuration.rehello(sequence.getSequence());
        }
        
        buffer.put(message.toString().getBytes());
        buffer.flip();
        channel.write(buffer, attachment, handler);
    }

    @Override
    public void failed(Throwable exc, SmbsClient attachment) {
        log.error("ERROR EXECUTED IN CONNECT: {}", exc);
        var emitter = attachment.getEmitter();
        emitter.error(exc);
    }
    
}
