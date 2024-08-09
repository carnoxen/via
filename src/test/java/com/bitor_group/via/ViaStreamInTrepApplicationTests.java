package com.bitor_group.via;

import java.io.FileReader;
import java.io.IOException;
// import java.io.StringReader;
import java.util.Arrays;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
// import org.springframework.boot.test.context.SpringBootTest;

// @SpringBootTest
class ViaStreamInTrepApplicationTests {
    
    @Test
    void contextLoads() throws IOException {
        var reader = new FileReader("src\\test\\java\\com\\bitor_group\\via\\input.csv");
        System.out.println(reader.toString());
        var columns = CSVFormat.RFC4180.builder().setCommentMarker('#').setHeader().setSkipHeaderRecord(true).build().parse(reader);
        System.out.println(columns.getHeaderNames());
        for (var record: columns) {
            System.out.println(Arrays.toString(record.values()));
        }
    }

}
