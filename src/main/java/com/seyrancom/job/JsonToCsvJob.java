package com.seyrancom.job;

import com.seyrancom.dto.Segment;
import com.seyrancom.dto.Visitor;
import com.seyrancom.dto.VisitorSegment;
import com.seyrancom.service.SparkService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class JsonToCsvJob implements SparkJob, Serializable {
    @Override
    public void saveMetrics(SparkService.Context context) {
        //meterRegistry.counter("received.messages").increment(sparkResultsCounter.value());
    }

    @Override
    public void process(SparkService.Context context) throws Exception {

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("visitorIds", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
        });

        Dataset<Segment> ds = context.getSession()
                .readStream()
                .format(FORMAT_JSON)
                .schema(schema)
                .json(context.getAppProperties().getInputPath())
                .as(Encoders.bean(Segment.class));


/*        ds.flatMap(Segment::getVisitorIdsMappedListIterator, Encoders.bean(VisitorSegment.class))
                .groupByKey((MapFunction<VisitorSegment, String>) VisitorSegment::getId, Encoders.STRING())
                .mapValues((MapFunction<VisitorSegment, String>) VisitorSegment::getSegmentId, Encoders.STRING())
                .agg(array_join(collect_list(col(("id"))), ";").as("segmentIds"));*/

        ds.writeStream()
                .option(SPARK_OUTPUT_PATH_KEY, context.getAppProperties().getOutputPath())
                .option(SPARK_CHECKPOINT_LOCATION_KEY, context.getSparkProperties().getCheckpointPath())
                //.format(FORMAT_CSV)
                .format(FORMAT_CONSOLE)
                .trigger(Trigger.ProcessingTime(context.getSparkProperties().getWindowSeconds(), TimeUnit.SECONDS))
                //.partitionBy("key")
                .start()
                .awaitTermination();
    }
}
