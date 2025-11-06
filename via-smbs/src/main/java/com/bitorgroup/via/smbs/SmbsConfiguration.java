package com.bitorgroup.via.smbs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.bitorgroup.via.common.SmbsMessage;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
enum Operation {
    HELLO(90001),
    REHELLO(90002), 
    DATA(20000), 
    CLOSE(90099);

    private final Integer value;

    public Integer res() {
        return this.value + 1000;
    }
}

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
enum Status {
    MISSED(5), 
    DUPLICATED(6);

    private final Integer value;
}

@Data
@Slf4j
@Configuration
@ConfigurationProperties("smbs")
public class SmbsConfiguration {

    private String host;
    private String record;
    private Integer firm;

    public SmbsConfiguration(String host, String record, Integer firm) {
        this.host = host;
        this.record = record;
        this.firm = firm;
    }

    public SmbsMessage parseToMessage(ByteBuffer buffer) {
        var message = new SmbsMessage();

        var byteLength = 6;
        var newBytes = new byte[byteLength];
        buffer.get(newBytes, 0, byteLength);
        message.setLength(Integer.valueOf(new String(newBytes)));

        byteLength = 8;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setTrade(new String(newBytes));

        byteLength = 5;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setFirm(Integer.valueOf(new String(newBytes)));

        byteLength = 6;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setOperation(Integer.valueOf(new String(newBytes)));

        byteLength = 4;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setStatus(Integer.valueOf(new String(newBytes)));

        byteLength = 4;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setData(new String(newBytes));

        byteLength = 14;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        var date = LocalDateTime.parse(new String(newBytes), SmbsMessage.DATE_FORMAT);
        message.setTimestamp(date);

        byteLength = 8;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setSequence(Integer.valueOf(new String(newBytes)));

        byteLength = 15;
        newBytes = new byte[byteLength];
        buffer.get(newBytes, buffer.position(), byteLength);
        message.setCustom(new String(newBytes));

        if (buffer.hasRemaining()) {
            try (final var reader = Files.newBufferedReader(Paths.get(record))) {
                var format = CSVFormat.RFC4180.builder()
                    .setHeader()
                    .setCommentMarker('#')
                    .setSkipHeaderRecord(true)
                    .build().parse(reader);

                var rows = format.stream()
                    .collect(Collectors
                            .toMap(x -> x.get("ID"), x -> Integer.valueOf(x.get("LENGTH"))));

                for (var row: rows.entrySet()) {
                    byteLength = row.getValue();
                    newBytes = new byte[byteLength];
                    buffer.get(newBytes, buffer.position(), byteLength);
                    message.getContent().put(row.getKey(), new String(newBytes));
                }
            } catch (IOException e) {
                log.error("RECORD FILE NOT FOUND OR GRAMMER ISSUE: {}", e);
            }
        }

        return message;
    }

    private SmbsMessage makeCommonMessage() {
        var message = new SmbsMessage();
        var trade = record.replace("\\.\\w+", "");

        message.setTrade(trade);
        message.setFirm(firm);

        return message;
    }

    public SmbsMessage hello() {
        return makeCommonMessage();
    }

    public SmbsMessage rehello(int sequence) {
        var message = makeCommonMessage();
        var operation = Operation.REHELLO.getValue();

        message.setOperation(operation);
        message.setSequence(sequence);

        return message;
    }

    public SmbsMessage data(int sequence) {
        var message = makeCommonMessage();
        var operation = Operation.DATA.res();

        message.setOperation(operation);
        message.setSequence(sequence);

        return message;
    }

    public SmbsMessage close(int sequence) {
        var message = makeCommonMessage();
        var operation = Operation.CLOSE.res();

        message.setOperation(operation);
        message.setSequence(sequence);

        return message;
    }

    public SmbsMessage missed(int sequence) {
        var message = makeCommonMessage();
        var operation = Operation.DATA.res();
        var status = Status.MISSED.getValue();

        message.setOperation(operation);
        message.setSequence(sequence);
        message.setStatus(status);

        return message;
    }

    public SmbsMessage duplicated(int sequence) {
        var message = makeCommonMessage();
        var operation = Operation.DATA.res();
        var status = Status.DUPLICATED.getValue();

        message.setOperation(operation);
        message.setSequence(sequence);
        message.setStatus(status);

        return message;
    }

}
