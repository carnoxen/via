package com.bitorgroup.via.smbs.handler;

import java.nio.channels.CompletionHandler;

import com.bitorgroup.via.smbs.SmbsClient;

public interface SmbsHandler<V> extends CompletionHandler<V, SmbsClient> {}
