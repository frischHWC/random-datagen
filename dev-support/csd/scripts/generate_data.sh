#!/bin/bash

CMD=$1
SERVER_PORT=$2

set -x
. ${COMMON_SCRIPT}
PYTHON_COMMAND_INVOKER=${PYTHON_COMMAND_INVOKER:-/usr/bin/python}

case $CMD in
 (gen_customer_hdfs_ozone_hive)
     echo "Starting to Generate Customer data to HDFS in Parquet & Hive"

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-chinal-model.json 10000 12 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-china-model.json"
       exit 1
     fi

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10000 9 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-germany-model.json 10000 9 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-germany-model.json"
       exit 1
     fi

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-india-model.json 10000 9 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-india-model.json"
       exit 1
     fi

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-italy-model.json 10000 3 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-italy-model.json"
       exit 1
     fi

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-japan-model.json 10000 11 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-japan-model.json"
       exit 1
     fi

     EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-spain-model.json 10000 4 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-spain-model.json"
       exit 1
     fi

    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-turkey-model.json 10000 12 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-turkey-model.json"
       exit 1
     fi

    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-usa-model.json 10000 21 3600 hdfs-parquet ozone-parquet hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-usa-model.json"
       exit 1
     fi

     echo "Finished to Generate Customer data to HDFS in Parquet & Hive"
     exit 0
     ;;

  (gen_transaction_hbase)
    echo "Starting to Generate Transaction Data into HBase"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/transaction/transaction-model.json 10000 100 3600 hbase)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for transaction-model.json"
       exit 1
     fi
     echo "Finished to Generate Transaction Data into HBase"
    ;;

  (gen_sensor_hive)
    echo "Starting to Generate sensor Data into Hive"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/plant-model.json 100 1 3600 hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for plant-model.json"
       exit 1
     fi

    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model.json 10000 10 3600 hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for sensor-model.json"
       exit 1
     fi

    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model.json 10000 100 3600 hive)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for sensor-data-model.json"
       exit 1
     fi
    echo "Finished to Generate sensor Data into Hive"
    ;;

  (gen_ps_kafka_kudu)
    echo "Starting to Generate public service Data into Kafka"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/incident-model.json 1000 1000 3600 kafka)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for incident-model.json"
       exit 1
     fi
    echo "Finished to Generate public service Data into Kafka"

    echo "Starting to Generate public service Data into Kafka and Kudu"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/sensor-data-model.json 10000 10 3600 kafka kudu)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for intervention-team-model.json"
       exit 1
     fi
    echo "Finished to Generate public service Data into Kafka and Kudu"
    ;;

  (gen_weather_solr_kafka)
    echo "Starting to Generate Weather Data into SolR"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/weather-model.json 10000 100 3600 solr)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for weather-model.json"
       exit 1
     fi
    echo "Finished to Generate Weather Data into SolR"

    echo "Starting to Generate Weather Measures Data into Kafka"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/public_service/weather-sensor-model.json 1000 100 3600 kafka)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for weather-sensor-model.json"
       exit 1
     fi
    echo "Finished to Generate Weather Measures Data into Kafka"
    ;;

  (gen_local_data)
    echo "Starting to Generate Local data for test purposes"
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/customer/customer-france-model.json 10 1 3600 json)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for customer-france-model.json"
       exit 1
     fi
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/finance/transaction-model.json 10 1 3600 csv)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for transaction-model.json"
       exit 1
     fi
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/plant-model.json 10 1 3600 csv)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for plant-model.json"
       exit 1
     fi
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-model.json 10 1 3600 json)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for sensor-model.json"
       exit 1
     fi
    EXIT_CODE=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/generate_data.py ${SERVER_PORT} /opt/cloudera/parcels/DATAGEN/models/industry/sensor-data-model.json 10 1 3600 csv)
     if [ "${EXIT_CODE}" == "1" ]
     then
       echo " Unable to generate data for sensor-data-model.json"
       exit 1
     fi

    echo "Finished to Generate Local data for test purposes"
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
