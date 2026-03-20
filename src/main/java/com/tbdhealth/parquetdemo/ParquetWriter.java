package com.tbdhealth.parquetdemo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class ParquetWriter {

    private static final String SCHEMA_JSON =
        """
        {
          "type": "record",
          "name": "OrderRecord",
          "namespace": "com.tbdhealth.parquetdemo",
          "fields": [
            {"name": "orderId",               "type": "string"},
            {"name": "email",                 "type": "string"},
            {"name": "region",                "type": "string"},
            {"name": "product",               "type": "string"},
            {"name": "quantity",              "type": "int"},
            {"name": "unitPrice",             "type": "double"},
            {"name": "orderStatus",           "type": "string"},
            {"name": "fulfillmentStatus",     "type": "string"},
            {"name": "storeSourceType",       "type": "string"},
            {"name": "orderCreateDate",       "type": "string"},
            {"name": "vendor",                "type": "string"},
            {"name": "kitId",                 "type": "string"},
            {"name": "outboundTrackingNumber","type": "string"},
            {"name": "inboundTrackingNumber", "type": "string"}
          ]
        }
        """;

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    public void write(List<OrderRecord> records, Path outputPath) throws IOException {
        Configuration hadoopConf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath =
            new org.apache.hadoop.fs.Path(outputPath.toAbsolutePath().toString());

        FileSystem fs = FileSystem.getLocal(hadoopConf);
        if (fs.exists(hadoopPath)) {
            fs.delete(hadoopPath, false);
        }

        try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer =
            AvroParquetWriter.<GenericRecord>builder(
                    HadoopOutputFile.fromPath(hadoopPath, hadoopConf))
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (OrderRecord r : records) {
                GenericRecord avro = new GenericData.Record(SCHEMA);
                avro.put("orderId",                r.getOrderId());
                avro.put("email",                  r.getEmail());
                avro.put("region",                 r.getRegion());
                avro.put("product",                r.getProduct());
                avro.put("quantity",               r.getQuantity());
                avro.put("unitPrice",              r.getUnitPrice());
                avro.put("orderStatus",            r.getOrderStatus());
                avro.put("fulfillmentStatus",      r.getFulfillmentStatus());
                avro.put("storeSourceType",        r.getStoreSourceType());
                avro.put("orderCreateDate",        r.getOrderCreateDate());
                avro.put("vendor",                 r.getVendor());
                avro.put("kitId",                  r.getKitId());
                avro.put("outboundTrackingNumber", r.getOutboundTrackingNumber());
                avro.put("inboundTrackingNumber",  r.getInboundTrackingNumber());
                writer.write(avro);
            }
        }
    }
}
