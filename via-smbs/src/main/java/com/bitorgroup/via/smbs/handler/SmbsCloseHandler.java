package com.bitorgroup.via.smbs.handler;

import org.springframework.stereotype.Service;

import com.bitorgroup.via.smbs.SmbsClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SmbsCloseHandler implements SmbsHandler<Integer> {

    @Override
    public void completed(Integer result, SmbsClient attachment) {
        var emitter = attachment.getEmitter();
        emitter.complete();
    }

    @Override
    public void failed(Throwable exc, SmbsClient attachment) {
        log.error("ERROR EXECUTED IN CLOSE: {}", exc);
        var emitter = attachment.getEmitter();
        emitter.error(exc);
    }
    
}
