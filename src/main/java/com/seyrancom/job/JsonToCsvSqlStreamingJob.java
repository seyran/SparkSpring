package com.seyrancom.job;

import com.seyrancom.service.SparkService;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class JsonToCsvSqlStreamingJob implements SparkJob, Serializable {
    @Override
    public void saveMetrics(SparkService.Context context) {
        //meterRegistry.counter("received.messages").increment(sparkResultsCounter.value());
    }

    @Override
    public void process(SparkService.Context context) throws Exception {

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("visitorIds", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
                ,
                new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty())
        });

        Dataset<Row> segmentDataset = context.getSession()
                .readStream()
                .format(FORMAT_JSON)
                .schema(schema)
                .json(context.getAppProperties().getInputPath())
                ;

        Dataset<Row> visitorDataset = segmentDataset
                .withColumn("visitorId", explode(segmentDataset.col("visitorIds")))
                .drop("visitorIds")
                .withWatermark("timestamp", "10 seconds")
                .groupBy(col("visitorId")
                        ,
                        window(col("timestamp"), "1 day")

                ).agg(collect_list(col(("id"))).as("segmentIds"))
                .select("visitorId", "segmentIds")
                ;


        visitorDataset
                .writeStream()
//                .option(SPARK_OUTPUT_PATH_KEY, context.getAppProperties().getOutputPath())
//                .option(SPARK_CHECKPOINT_LOCATION_KEY, context.getSparkProperties().getCheckpointPath())
//                .format("csv")
//                .partitionBy("visitorId")
                .format(FORMAT_CONSOLE)
//                .outputMode("update")
                .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
//                .partitionBy("key")
                .start()
                .awaitTermination();
        ;
    }
}
