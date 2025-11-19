package com.example.transfers_batch_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TransfersBatchServiceApplication {

    public static void main(String[] args) {
        // Set hadoop home
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        // Disable file system caching
        System.setProperty("fs.hdfs.impl.disable.cache", "true");

        SpringApplication.run(TransfersBatchServiceApplication.class, args);
    }
}
