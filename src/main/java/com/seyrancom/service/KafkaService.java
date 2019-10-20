package com.seyrancom.service;

import com.seyrancom.config.KafkaProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.function.Consumer;


@Service
public class KafkaService {

    @Autowired
    private KafkaProperties kafkaProperties;

    public void runStream(Consumer<KStream<String, String>> handler) {
        StreamsBuilder builder = new StreamsBuilder();
        handler.accept(builder.stream(kafkaProperties.getTopicName()));
        new KafkaStreams(builder.build(), getConfigurations()).start();
    }

    private Properties getConfigurations() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaProperties.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapAddress());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
