package com.seyrancom.producer;

import com.seyrancom.config.KafkaProperties;
import com.seyrancom.dto.Record;
import com.seyrancom.dto.RecordBody;
import com.seyrancom.service.KafkaMessageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Slf4j
@Service
@Profile("stg-kafka")
public class KafkaProducer {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaMessageService kafkaMessageService;
    private final KafkaProperties kafkaProperties;

    @Autowired
    public KafkaProducer(KafkaMessageService kafkaMessageService, KafkaProperties kafkaProperties) {
        this.kafkaMessageService = kafkaMessageService;
        this.kafkaProperties = kafkaProperties;
    }

    @Scheduled(fixedRate = 1000)
    void sendMessage() throws JsonProcessingException {

        String uuid = UUID.randomUUID().toString();
        Record record = new Record();
        record.setId(uuid);

        RecordBody body = new RecordBody();

        int low = 20;
        int high = 50;
        int rnd = new Random().nextInt(high - low) + low;

        List<String> footers = IntStream.range(0, rnd)
                .mapToObj(i -> String.format("Footer_%s", i))
                .collect(Collectors.toList());

        body.setFooters(footers);
        body.setHeader("Header");

        record.setBody(body);
        String accountId = "account_" + new Random().nextInt(3);
        kafkaMessageService.sendMessage(kafkaProperties.getTopicName(), accountId, OBJECT_MAPPER.writeValueAsString(record));
    }
}
