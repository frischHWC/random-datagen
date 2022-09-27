#!/bin/bash
CMD=$1

case $CMD in
  (start)
    echo "Starting DATAGEN"
    envsubst < "${CONF_DIR}/service.properties" > "${CONF_DIR}/service.properties.tmp"
    mv "${CONF_DIR}/service.properties.tmp" "${CONF_DIR}/service.properties"
    exec ${JAVA_HOME}/bin/java -jar -Dspring.profiles.active=cdp -Dserver.port=${SERVER_PORT} -Xmx${MAX_HEAP_SIZE}G ${DATAGEN_JAR_PATH} --spring.config.location=file:${CONF_DIR}/service.properties
    ;;
 (gen_customer_hdfs_ozone_hive)
    SERVER_PORT=$2
     echo "Starting to Generate Customer data to HDFS in Parquet & Hive"
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-china-model.json&rows=10000&batches=12"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-france-model.json&rows=10000&batches=9"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-germany-model.json&rows=10000&batches=9"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-india-model.json&rows=10000&batches=19"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-italy-model.json&rows=10000&batches=3"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-japan-model.json&rows=10000&batches=11"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-spain-model.json&rows=10000&batches=4"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-turkey-model.json&rows=10000&batches=12"
     sleep 30
     curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/multiplesinks?sinks=hdfs-parquet&sinks=hive&sinks=ozone-parquet&model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-usa-model.json&rows=10000&batches=21"
     echo "Finished to Generate Customer data to HDFS in Parquet & Hive"
     exit 0
     ;;
  (gen_transaction_hbase)
    SERVER_PORT=$2
    echo "Starting to Generate Transaction Data into HBase"
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/hbase?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Ffinance%2Ftransaction-model.json&rows=10000&batches=100"
    echo "Finished to Generate Transaction Data into HBase"
    ;;
  (gen_sensor_hive)
    SERVER_PORT=$2
    echo "Starting to Generate sensor Data into Hive"
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/hive?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Findustry%2Fplant-model.json&rows=100&0batches=1"
    sleep 30
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/hive?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Findustry%2Fsensor-model.json&rows=10000&batches=10"
    sleep 30
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/hive?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Findustry%2Fsensor-data-model.json&rows=100000&batches=100"
    echo "Finished to Generate sensor Data into Hive"
    ;;
  (gen_local_data)
    SERVER_PORT=$2
    echo "Starting to Generate Local data for test purposes"
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/json?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Fcustomer%2Fcustomer-france-model.json&rows=10&batches=1"
    sleep 5
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/csv?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Ffinance%2Ftransaction-model.json&rows=10&batches=1"
    sleep 5
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/csv?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Findustry%2Fplant-model.json&rows=10&batches=1"
    sleep 5
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/json?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Findustry%2Fsensor-model.json&rows=10&batches=1"
    sleep 5
    curl -X POST -H  "accept: */*" "http://localhost:${SERVER_PORT}/datagen/csv?model=%2Fopt%2Fcloudera%2Fparcels%2FDATAGEN%2Fmodels%2Findustry%2Fsensor-data-model.json&rows=10&batches=1"
    echo "Finished to Generate Local data for test purposes"
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac