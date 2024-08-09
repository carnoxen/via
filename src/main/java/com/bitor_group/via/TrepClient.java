package com.bitor_group.via;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class TrepClient implements OmmConsumerClient {
    private long handle = -1;
    private TrepConfiguration trepConfiguration;
    private OmmConsumer consumer;
    private BlockingQueue<String> queue;
    private List<String> columns;
    private Map<String, CSVRecord> rows;
    private Map<String, Map<String, String>> recordMap = new ConcurrentHashMap<>();

    public TrepClient(TrepConfiguration trepConfiguration) {
        this.trepConfiguration = trepConfiguration;
        this.consumer = EmaFactory.createOmmConsumer(EmaFactory.createOmmConsumerConfig()
                .host(this.trepConfiguration.getHost())
                .username(this.trepConfiguration.getUsername()));
    }

    public void start(BlockingQueue<String> queue) {
        try (var reader = Files.newBufferedReader(Paths.get(this.trepConfiguration.getRecord()),
                StandardCharsets.UTF_8)) {
            this.queue = queue;
            var inputFormat = CSVFormat.RFC4180.builder()
                    .setHeader()
                    .setCommentMarker('#')
                    .setSkipHeaderRecord(true)
                    .build().parse(reader);
            this.columns = inputFormat.getHeaderNames();
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
            log.error("INPUT FORMAT ERROR:", e);
        }
    }

    public void close() {
        this.consumer.unregister(this.handle);
    }

    @Override
    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent consumerEvent) {
        String itemName = refreshMsg.hasName() ? refreshMsg.name() : "<not set>";
        String serviceName = refreshMsg.hasServiceName() ? refreshMsg.serviceName() : "<not set>";
        log.info("Item Name: {}", itemName);
        log.info("Service Name: {}", serviceName);

        log.info("Item State: " + refreshMsg.state());

        if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType()) {
            this.recordMap.putIfAbsent(itemName, new ConcurrentHashMap<>());
            decode(itemName, refreshMsg.payload().fieldList());
        }
    }

    @Override
    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent consumerEvent) {
        String itemName = updateMsg.hasName() ? updateMsg.name() : "<not set>";
        String serviceName = updateMsg.hasServiceName() ? updateMsg.serviceName() : "<not set>";
        log.info("Item Name: {}", itemName);
        log.info("Service Name: {}", serviceName);

        if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType()) {
            this.recordMap.putIfAbsent(itemName, new ConcurrentHashMap<>());
            decode(itemName, updateMsg.payload().fieldList());
        }
    }

    @Override
    public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent consumerEvent) {
        log.info("Unsupported msg: STA");
    }

    @Override
    public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent) {
        log.info("Unsupported msg: GEN");
    }

    @Override
    public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent) {
        log.info("Unsupported msg: ACK");
    }

    @Override
    public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent) {
        log.info("Unsupported msg: ALL");
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

                log.info("  Fid: {} Name: {} DataType: {} Value: {}",
                        fieldEntry.fieldId(), fieldName, DataType.asString(fieldEntry.load().dataType()), data);

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
                .setHeader(this.columns.toArray(String[]::new))
                .build();

        try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
            this.rows.entrySet().forEach(x -> {
                String itemName = x.getKey();
                var record = x.getValue();
                try {
                    printer.printRecord(record.stream()
                    .map(r -> (r.startsWith("@")) ? recordMap.get(itemName).get(r) : r));
                } catch (IOException e) {
                    log.error("CSV PRINTER FAILED: ", e);
                    log.error("See the next record");
                }
            });
        } catch (Exception e) {
            log.error("CSV PRINTER CONSTRUCTION FAILED: ", e);
        }

        try {
            queue.put(writer.toString());
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.recordMap.clear();
    }

}
