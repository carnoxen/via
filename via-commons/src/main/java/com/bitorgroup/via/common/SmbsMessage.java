package com.bitorgroup.via.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Hashtable;
import java.util.Map;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SmbsMessage {

    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter
            .ofPattern("yyyyMMddHHmmss");

    // length: 6, kind: Integer
    private Integer length = 70;
    // length: 8, kind: String
    private String trade;
    // length: 5, kind: Integer
    private Integer firm;
    // length: 6, kind: Integer
    private Integer operation = 90001;
    // length: 4, kind: Integer
    private Integer status = 0;
    // length: 4, kind: String
    private String data = "";
    // length: 14, kind: Date
    private LocalDateTime timestamp = LocalDateTime.now();
    // length: 8, kind: Integer
    private Integer sequence = 0;
    // length: 15, kind: String
    private String custom = "";

    private Map<String, String> content = new Hashtable<>();

    @Override
    public String toString() {
        var timestampString = timestamp.format(DATE_FORMAT);
        String result = "%06d%-8s%05d%06d%04d%4s%14s%08d%15s"
                .formatted(length, trade, firm, operation, status, data,
                        timestampString, sequence, custom);
        return result;
    }

}
