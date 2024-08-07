package com.bitor_group.via;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.Data;
import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.DataType.DataTypes;
import com.refinitiv.ema.access.ElementList;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.Msg;
import com.refinitiv.ema.access.OmmArray;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;
import com.refinitiv.ema.rdm.EmaRdm;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class TrepClient implements OmmConsumerClient {
    private long handle = -1;
    private StreamBridge streamBridge;
    private TrepConfiguration trepConfiguration;
    private OmmConsumer consumer;
    private List<String> columns;
    private Map<String, CSVRecord> rows;
    private Map<String, Map<String, String>> recordMap = new ConcurrentHashMap<>();

    public TrepClient(StreamBridge streamBridge, TrepConfiguration trepConfiguration) {
        this.streamBridge = streamBridge;
        this.trepConfiguration = trepConfiguration;
        this.consumer = EmaFactory.createOmmConsumer(EmaFactory.createOmmConsumerConfig()
                .host(this.trepConfiguration.getHost())
                .username(this.trepConfiguration.getUsername()));
    }

    public void start() {
        try (
                var reader = Files.newBufferedReader(Paths.get(this.trepConfiguration.getRecord()),
                        StandardCharsets.UTF_8)) {
            var inputFormat = CSVFormat.RFC4180.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build().parse(reader);
            this.columns = CSVFormat.RFC4180.builder().build().parse(reader).getHeaderNames();
            inputFormat
                .forEach(x -> this.rows.put(x.get("RIC"), x));

            OmmArray array = EmaFactory.createOmmArray();
            this.rows.keySet()
                .forEach(x -> array.add(EmaFactory.createOmmArrayEntry().ascii(x)));

            ElementList batch = EmaFactory.createElementList();
            batch.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, array));

            this.handle = consumer.registerClient(EmaFactory.createReqMsg()
                    .serviceName(this.trepConfiguration.getService())
                    .payload(batch), this);

        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public void close() {
        if (this.handle != -1) {
            this.consumer.unregister(this.handle);
        }
    }

    @Override
    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent consumerEvent) {
        String itemName = refreshMsg.hasName() ? refreshMsg.name() : "<not set>";
        String serviceName = refreshMsg.hasServiceName() ? refreshMsg.serviceName() : "<not set>";
        log.info("Item Name: {}", itemName);
        log.info("Service Name: {}", serviceName);

        log.info("Item State: " + refreshMsg.state());

        if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType())
            decode(itemName, refreshMsg.payload().fieldList());
    }

    @Override
    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent consumerEvent) {
        String itemName = updateMsg.hasName() ? updateMsg.name() : "<not set>";
        String serviceName = updateMsg.hasServiceName() ? updateMsg.serviceName() : "<not set>";
        log.info("Item Name: {}", itemName);
        log.info("Service Name: {}", serviceName);

        if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType())
            decode(itemName, updateMsg.payload().fieldList());
    }

    @Override
    public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent consumerEvent) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onStatusMsg'");
    }

    @Override
    public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onGenericMsg'");
    }

    @Override
    public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onAckMsg'");
    }

    @Override
    public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onAllMsg'");
    }

    private void decode(String itemName, FieldList fieldList) {
        for (FieldEntry fieldEntry : fieldList) {
            String fieldName = fieldEntry.name();
            String data = "";

            if (Data.DataCode.BLANK == fieldEntry.code()) {
                log.info(" blank");
            } else {
                switch (fieldEntry.loadType()) {
                    case DataTypes.REAL:
                        data = "" + fieldEntry.real().asDouble();
                        break;
                    case DataTypes.DATE:
                        data = "" + fieldEntry.date().day() + " / " + fieldEntry.date().month() + " / "
                                + fieldEntry.date().year();
                        break;
                    case DataTypes.TIME:
                        data = "" + fieldEntry.time().hour() + ":" + fieldEntry.time().minute() + ":"
                                + fieldEntry.time().second() + ":" + fieldEntry.time().millisecond();
                        break;
                    case DataTypes.INT:
                        data = "" + fieldEntry.intValue();
                        break;
                    case DataTypes.UINT:
                        data = "" + fieldEntry.uintValue();
                        break;
                    case DataTypes.ASCII:
                        data = "" + fieldEntry.ascii();
                        break;
                    case DataTypes.ENUM:
                        data = "" + (fieldEntry.hasEnumDisplay() ? fieldEntry.enumDisplay() : fieldEntry.enumValue());
                        break;
                    case DataTypes.RMTES:
                        data = "" + fieldEntry.rmtes();
                        break;
                    case DataTypes.ERROR:
                        data = "(" + fieldEntry.error().errorCodeAsString() + ")";
                        break;
                    default:
                        break;
                }

                log.info("Fid: {} Name = {} DataType: {} Value: {}", 
                    fieldEntry.fieldId(), fieldName, DataType.asString(fieldEntry.load().dataType()), data);

                this.recordMap.putIfAbsent(itemName, new ConcurrentHashMap<>());
                this.recordMap.get(itemName).put("@" + fieldName, data);
            }
        }

        if (this.recordMap.size() == this.rows.size()) {
            sendData();
        }
    }

    private void sendData() {
        StringWriter writer = new StringWriter();
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
            .setHeader(this.columns.toArray(new String[0]))
            .build();

        try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
            this.rows.entrySet().forEach(x -> {
                String itemName = x.getKey();
                var record = x.getValue();
                try {
                    for (var col: columns) {
                        String target = record.get(col);
                        if (target.startsWith("@")) {
                            target = this.recordMap.get(itemName).get(target);
                        }

                        printer.print(target);
                    }
                    printer.println();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        this.streamBridge.send("trep-sinc", writer.toString());
        this.recordMap.clear();
    }

}
