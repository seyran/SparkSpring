package com.seyrancom.config;

import com.seyrancom.config.spring.YamlPropertyLoaderFactory;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Data
@Component
@PropertySource(value = "classpath:spark-${spring.profiles.active}.yml", factory = YamlPropertyLoaderFactory.class)
@ConfigurationProperties("spark")
public class SparkProperties implements Serializable {

    private String applicationName;
    private String master;
    private String bootstrapAddress;
    private String applicationId;
    private String checkpointPath;
    private int windowSeconds;
    private String kafkaTopicName;
    private String metricsConfDir;
}
