package com.seyrancom.job;

import com.seyrancom.service.SparkService;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class JsonToCsvSqlJob implements SparkJob, Serializable {
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

        Dataset<Row> segmentDataset = context.getSession()
                .read()
                .format(FORMAT_JSON)
                .schema(schema)
                .json(context.getAppProperties().getInputPath());

        Dataset<Row> visitorDataset = segmentDataset
                .withColumn("visitorId", explode(segmentDataset.col("visitorIds")))
                .drop("visitorIds")
                .groupBy(col("visitorId"))
                .agg(array_join(collect_list(col(("id"))), ";").as("segmentIds"));

        visitorDataset
                .write()
                .csv(context.getAppProperties().getOutputPath())
        ;
    }
}
