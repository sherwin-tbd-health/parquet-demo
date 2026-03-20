package com.tbdhealth.parquetdemo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Component
public class DemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    @Value("${demo.s3.bucket}")
    private String bucket;

    @Value("${demo.s3.region:us-east-1}")
    private String region;

    @Override
    public void run(String... args) throws IOException {
        // 1. Generate data
        log.info("=== Step 1: Generating 1,000 order records ===");
        List<OrderRecord> orders = OrderDataGenerator.generate();
        log.info("Generated {} records", orders.size());

        // 2. Write CSV
        log.info("=== Step 2: Writing CSV ===");
        Path csvPath = Path.of("orders.csv");
        new CsvWriter().write(orders, csvPath);
        log.info("CSV size: {} KB", Files.size(csvPath) / 1024);

        // 3. Write Parquet
        log.info("=== Step 3: Writing Parquet (Snappy compressed) ===");
        Path parquetPath = Path.of("orders.parquet");
        new ParquetWriter().write(orders, parquetPath);
        log.info("Parquet size: {} KB", Files.size(parquetPath) / 1024);

        long csvBytes = Files.size(csvPath);
        long parquetBytes = Files.size(parquetPath);
        log.info(
            "=== Size comparison: CSV={} KB Parquet={} KB ({}x smaller) ===",
            csvBytes / 1024,
            parquetBytes / 1024,
            String.format("%.1f", (double) csvBytes / parquetBytes));

        // 4. Upload to S3
        log.info("=== Step 4: Uploading to S3 bucket '{}' ===", bucket);
        S3Client s3 = S3Client.builder().region(Region.of(region)).build();
        S3Uploader uploader = new S3Uploader(s3, bucket);
        uploader.upload(csvPath, "csv/orders.csv");
        uploader.upload(parquetPath, "parquet/orders.parquet");
        log.info("=== Done! Next: run the Glue crawlers then query in Athena/QuickSight. ===");
    }
}
