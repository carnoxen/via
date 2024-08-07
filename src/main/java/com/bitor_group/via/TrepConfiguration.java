package com.bitor_group.via;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Configuration
@Data
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "trep")
public class TrepConfiguration {
    private final String host;
    private final String username;
    private final String service;
    private final String record;
}
