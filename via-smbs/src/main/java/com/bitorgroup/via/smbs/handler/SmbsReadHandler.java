package com.bitorgroup.via.smbs.handler;

import org.springframework.stereotype.Service;

import com.bitorgroup.via.smbs.SmbsClient;
import com.bitorgroup.via.smbs.SmbsSequence;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SmbsReadHandler implements SmbsHandler<Integer> {

    private SmbsCloseHandler closeHandler;
    private SmbsWriteHandler writeHandler;

    public SmbsReadHandler(
        SmbsCloseHandler closeHandler,
        SmbsWriteHandler writeHandler
    ) {
        this.closeHandler = closeHandler;
        this.writeHandler = writeHandler;
    }

    @Override
    public void completed(Integer result, SmbsClient attachment) {
        var channel = attachment.getChannel();
        var buffer = attachment.getBuffer();
        var repository = attachment.getRepository();
        var configuration = attachment.getConfiguration();

        var record = configuration.getRecord();

        buffer.flip();
        var message = attachment.getConfiguration().parseToMessage(buffer);
        buffer.clear();

        var sequence = repository.findById(record).get();
        var sequenceMessage = message.getSequence();

        switch (message.getOperation()) {
            case 91001, 91002 -> {
                switch (message.getStatus()) {
                    case 0 -> {
                        log.info("SERVER HELLO SUCCEEDED. AWAITING DATA MESSAGE...");
                        channel.read(buffer, attachment, this);
                    }
                    case 1 -> {
                        log.info("RESTART REQUEST...");
    
                        sequence = SmbsSequence.builder()
                                .name(record)
                                .sequence(sequenceMessage).build();
                        repository.save(sequence);

                        message = configuration.rehello(sequenceMessage);

                        buffer.put(message.toString().getBytes());
                        buffer.flip();

                        channel.write(buffer, attachment, this.writeHandler);
                    }
                    default -> {
                        log.error("UNEXPECTED STATUS FROM SMBS {}", message.getStatus());
                    }
                }
            }
            case 20000 -> {
                log.info("DATA MESSAGE RECEIVED");

                if (sequenceMessage == sequence.getSequence()) {
                    log.info("IT'S CORRECT. SEND NORMAL RESPONSE...");

                    var emitter = attachment.getEmitter();
                    emitter.next(message.getContent().toString());
    
                    sequence = SmbsSequence.builder()
                            .name(record)
                            .sequence(sequenceMessage + 1).build();
                    repository.save(sequence);
                    
                    message = configuration.data(sequenceMessage);
                }
                else if (sequenceMessage > sequence.getSequence()) {
                    log.error("MISSED SEQUENCE.");
                    log.error("SEND ORIGINAL SEQUENCE...");

                    message = configuration.missed(sequence.getSequence());
                }
                else {
                    log.error("DUPLICATED SEQUENCE.");
                    log.error("SEND ORIGINAL SEQUENCE...");

                    message = configuration.duplicated(sequence.getSequence());
                }

                buffer.put(message.toString().getBytes());
                buffer.flip();

                channel.write(buffer, attachment, this.writeHandler);
            }
            case 90099 -> {
                log.info("CONNECTION IS CLOSING. SAY GOODBYE TO SMBS.");

                repository.deleteById(record);
                message = configuration.close(sequenceMessage);

                buffer.put(message.toString().getBytes());
                buffer.flip();

                channel.write(buffer, attachment, this.closeHandler);
            }
            default -> {
                log.error("UNEXPECTED OPERATION FROM SMBS {}", message.getOperation());
            }
        }
    }

    @Override
    public void failed(Throwable exc, SmbsClient attachment) {
        log.error("ERROR EXECUTED IN READ: {}", exc);
        var emitter = attachment.getEmitter();
        emitter.error(exc);
    }

}
