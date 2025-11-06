package com.bitorgroup.via.common;

import java.util.function.Consumer;

import reactor.core.publisher.FluxSink;

public interface ViaClient<T> extends Consumer<FluxSink<T>> {
    
}
