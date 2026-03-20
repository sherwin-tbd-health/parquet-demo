package com.tbdhealth.parquetdemo;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class CsvWriter {

    private static final String[] HEADERS = {
        "orderId", "email", "region", "product", "quantity", "unitPrice",
        "orderStatus", "fulfillmentStatus", "storeSourceType", "orderCreateDate",
        "vendor", "kitId", "outboundTrackingNumber", "inboundTrackingNumber"
    };

    public void write(List<OrderRecord> records, Path outputPath) throws IOException {
        try (CSVPrinter printer =
            new CSVPrinter(
                new FileWriter(outputPath.toFile()),
                CSVFormat.DEFAULT.builder().setHeader(HEADERS).build())) {

            for (OrderRecord r : records) {
                printer.printRecord(
                    r.getOrderId(),
                    r.getEmail(),
                    r.getRegion(),
                    r.getProduct(),
                    r.getQuantity(),
                    r.getUnitPrice(),
                    r.getOrderStatus(),
                    r.getFulfillmentStatus(),
                    r.getStoreSourceType(),
                    r.getOrderCreateDate(),
                    r.getVendor(),
                    r.getKitId(),
                    r.getOutboundTrackingNumber(),
                    r.getInboundTrackingNumber());
            }
        }
    }
}
