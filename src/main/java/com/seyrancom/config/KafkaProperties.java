package com.seyrancom.config;

import com.seyrancom.config.spring.YamlPropertyLoaderFactory;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Data
@Component
@PropertySource(value = "classpath:kafka-${spring.profiles.active}.yml", factory = YamlPropertyLoaderFactory.class)
@ConfigurationProperties("kafka")
public class KafkaProperties implements Serializable {

    private String bootstrapAddress;
    private String applicationId;
    private String topicName;

}
