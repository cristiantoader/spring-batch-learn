package org.cmtoader.learn;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;

@SpringBootApplication
@IntegrationComponentScan
@EnableBatchProcessing
public class SpringBatchSafaribooksApp {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchSafaribooksApp.class, args);
    }
}
