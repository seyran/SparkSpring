package com.seyrancom.job;

import com.seyrancom.service.SparkService;

public interface SparkJob {

    String FORMAT_JSON = "json";
    String FORMAT_CSV = "csv";
    String FORMAT_CONSOLE = "console";
    String SPARK_OUTPUT_PATH_KEY = "path";
    String SPARK_CHECKPOINT_LOCATION_KEY = "checkpointLocation";

    default void start(SparkService.Context context) {
    }

    void process(SparkService.Context context) throws Exception;

    default void end() {
    }

    default void saveMetrics(SparkService.Context context) {
    }
}
