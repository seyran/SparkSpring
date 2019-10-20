To access AWS S3 set Environment Variables:

AWS_ACCESS_KEY_ID = your_key
AWS_SECRET_ACCESS_KEY = your_secret
AWS_REGION = your_region

To write to your own directory on output sink set Environment Variable (for Windows users only):
USER = user_name

To use log4j-<profile>.properties set VM options:
-Dlog4j.configuration=log4j-<profile>.properties

**Spark Metrics**
1. Put metrics.properties into ${spark.metricsConfDir} or ${SPARK_HOME/conf} to enable metrics and JMX sink
2. You can find metrics HTTP GET: spark_host:spark_port/metrics/json (/metrics/application/json, /metrics/master/json, etc)
2. Put prometheus.yml into ${PROMETHEUS_CONF} and specify as config file when start prometheus
3. Put prometheus-config.yml and lib/jmx_prometheus_javaagent-0.11.0.jar into ${PROMETHEUS_CONF} to runn java agent which will publish metrics from JMX for Prometheus (read details here: https://github.com/prometheus/jmx_exporter)