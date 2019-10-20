package com.seyrancom;

import com.seyrancom.config.AppProperties;
import com.seyrancom.job.JsonToCsvJob;
import com.seyrancom.job.JsonToCsvSqlJob;
import com.seyrancom.job.JsonToCsvSqlStreamingJob;
import com.seyrancom.service.SparkService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ApplicationStartup {

    private final SparkService sparkService;
    private AppProperties appProperties;

    public ApplicationStartup(SparkService sparkService, AppProperties appProperties) {
        this.sparkService = sparkService;
        this.appProperties = appProperties;
    }

    @EventListener(ApplicationReadyEvent.class)
    private void start() {
        log.info("Starting application using properties {}", appProperties.toString());
        //sparkService.runJob(new AdobeToAmazonS3Job());
        //sparkService.runJob(new JsonToCsvSqlJob());
        sparkService.runJob(new JsonToCsvJob());
    }
}
