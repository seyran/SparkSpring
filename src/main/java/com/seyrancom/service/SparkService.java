package com.seyrancom.service;

import com.seyrancom.config.AppProperties;
import com.seyrancom.config.SparkProperties;
import com.seyrancom.job.SparkJob;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Service
@Getter(value = AccessLevel.PRIVATE)
public class SparkService {
    private final Context context;

    private List<SparkJob> jobList;
    private AtomicBoolean isInitialized;

    @Autowired
    private SparkService(AppProperties appProperties, SparkProperties sparkConfig, MeterRegistry meterRegistry) {
        context = new Context(appProperties, sparkConfig, null, meterRegistry);
        jobList = new CopyOnWriteArrayList();
        isInitialized = new AtomicBoolean();
    }

    synchronized private void init() {
        if (!isInitialized.get()) {
            isInitialized.set(true);
            Map<String, Object> consumerConfig = ImmutableMap.<String, Object>builder()
                    .put(ConsumerConfig.GROUP_ID_CONFIG, context.sparkProperties.getApplicationId())
                    .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.sparkProperties.getBootstrapAddress())
                    .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .build();
            SparkConf sparkConf = new SparkConf().setMaster(context.sparkProperties.getMaster())
                    .setAppName(context.sparkProperties.getApplicationName())
                    .set("spark.metrics.conf", context.sparkProperties.getMetricsConfDir())
                    .set("spark.sql.streaming.metricsEnabled", "true")
                    .set("spark.driver.extraJavaOptions", "-javaagent:$PROMETHEUS_CONF/jmx_prometheus_javaagent.jar=8085:$PROMETHEUS_CONF/prometheus-config.yml")
                    .set("spark.sql.streaming.metricsEnabled", Boolean.TRUE.toString());
            context.session = SparkSession.builder()
                    .config(sparkConf)
                    .getOrCreate();
            context.session.sparkContext().addSparkListener(new SparkListenerForJob());

            log.info("Initialized spark with configs: [{}]", context.sparkProperties);
        }
    }

    synchronized public void runJob(SparkJob job) {
        init();
        getJobList().add(job);

        log.info("Run job: {}", job.getClass().getName());
        try {
            job.process(context);
        } catch (Exception e) {
            log.error("Error in spark session:", e);
        } finally {
            context.getSession().stop();
        }
    }

    @AllArgsConstructor
    @Getter
    public class Context{
        private final AppProperties appProperties;
        private final SparkProperties sparkProperties;
        private SparkSession session;
        private final MeterRegistry meterRegistry;
    }

    private class SparkListenerForJob extends SparkListener {

        @Override
        public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
            log.info("Spark ApplicationStart Id: {}", applicationStart.appName());
        }

        @Override
        public void onJobStart(final SparkListenerJobStart jobStart) {
            log.info("Spark JobStart Id: {}", jobStart.jobId());
            getJobList().forEach(job -> job.start(context));
        }

        @Override
        public void onJobEnd(SparkListenerJobEnd jobEnd) {
            super.onJobEnd(jobEnd);
            log.info("Spark onJobEnd Id: {}", jobEnd.jobId());
            getJobList().forEach(job -> {
                        job.saveMetrics(context);
                        job.end();
                    }
            );
        }

        @Override
        public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
            log.info("Spark ApplicationEnd: {},", applicationEnd.time());
        }
    }
}
