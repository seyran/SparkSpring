package com.seyrancom.job;

import com.seyrancom.service.SparkService;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;
import scala.Serializable;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AdobeToAmazonS3Job implements SparkJob, Serializable {

    @Override
    public void process(SparkService.Context context) throws Exception {

        Set<String> topicsSet = ImmutableSet.of(context.getSparkProperties().getKafkaTopicName());
        Dataset<Row> dataset = context.getSession().readStream()
                .format("kafka")
                .option("kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getSparkProperties().getBootstrapAddress())
                .option("subscribe", String.join(", ", topicsSet))
                .load();


        Dataset<Row> rowDataset = dataset.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        rowDataset.groupBy("key").count();
        rowDataset.writeStream()
                .option(SPARK_OUTPUT_PATH_KEY, context.getAppProperties().getOutputPath())
                .option(SPARK_CHECKPOINT_LOCATION_KEY, context.getSparkProperties().getCheckpointPath())
                .option("mapreduce.output.basename", "acc")
                .format(FORMAT_JSON)
                .trigger(Trigger.ProcessingTime(context.getSparkProperties().getWindowSeconds(), TimeUnit.SECONDS))
                .partitionBy("key")
                .start()
                .awaitTermination();
    }
}
