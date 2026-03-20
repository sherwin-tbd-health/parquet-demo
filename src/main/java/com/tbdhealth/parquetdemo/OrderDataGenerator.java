package com.tbdhealth.parquetdemo;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class OrderDataGenerator {

    private static final String[] PRODUCTS = {
        "STI Test Kit", "PrEP Kit", "UTI Test Kit", "HPV Vaccine Kit"
    };
    private static final String[] REGIONS = {"CA", "NY", "TX", "FL", "WA"};
    private static final String[] STATUSES = {
        "PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"
    };
    private static final String[] FULFILLMENT_STATUSES = {
        "UNFULFILLED", "PARTIALLY_FULFILLED", "FULFILLED"
    };
    private static final String[] STORE_SOURCE_TYPES = {"SHOPIFY", "PARTNER", "INTERNAL"};
    private static final String[] VENDORS = {"vendor_a", "vendor_b", "vendor_c"};
    private static final int ROW_COUNT = 1000;

    public static List<OrderRecord> generate() {
        List<OrderRecord> records = new ArrayList<>(ROW_COUNT);
        LocalDate startDate = LocalDate.of(2024, 1, 1);

        for (int i = 0; i < ROW_COUNT; i++) {
            records.add(
                OrderRecord.builder()
                    .orderId(UUID.randomUUID().toString())
                    .email("patient" + i + "@example.com")
                    .region(REGIONS[i % REGIONS.length])
                    .product(PRODUCTS[i % PRODUCTS.length])
                    .quantity(i % 3 + 1)
                    .unitPrice(Math.round((29.99 + (i % 10) * 5.0) * 100.0) / 100.0)
                    .orderStatus(STATUSES[i % STATUSES.length])
                    .fulfillmentStatus(FULFILLMENT_STATUSES[i % FULFILLMENT_STATUSES.length])
                    .storeSourceType(STORE_SOURCE_TYPES[i % STORE_SOURCE_TYPES.length])
                    .orderCreateDate(startDate.plusDays(i % 365).toString())
                    .vendor(VENDORS[i % VENDORS.length])
                    .kitId("KIT-" + String.format("%05d", i + 1))
                    .outboundTrackingNumber("OUT-TRK-" + String.format("%08d", i + 1))
                    .inboundTrackingNumber("IN-TRK-" + String.format("%08d", i + 1))
                    .build());
        }

        return records;
    }
}
