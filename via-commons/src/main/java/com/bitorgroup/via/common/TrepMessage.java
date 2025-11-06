package com.bitorgroup.via.common;

import java.util.List;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@Builder
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class TrepMessage {
    private final String name;
    private final List<String> columns;
    private final List<List<String>> rows;
}
