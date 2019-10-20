package com.seyrancom.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Data
@Component
@ConfigurationProperties("app")
public class AppProperties implements Serializable {

    private int windowSeconds;
    private String inputPath;
    private String outputPath;
}
