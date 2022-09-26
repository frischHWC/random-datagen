#!/bin/bash

echo "STARTING INIT OF DATAGEN"

echo "RANGER_SERVICE: ${RANGER_SERVICE_NAME}"
echo "RANGER_URL: ${RANGER_URL}"
echo "RANGER_ADMIN_USER: ${RANGER_ADMIN_USER}"
echo "RANGER_ADMIN_PASSWORD: ${RANGER_ADMIN_PASSWORD}"

if [ "${RANGER_SERVICE_NAME}" != "" ] && [ "${RANGER_SERVICE_NAME}" != "none" ] && [ "${RANGER_URL}" != "" ] && [ "${RANGER_ADMIN_USER}" != "" ] && [ "${RANGER_ADMIN_PASSWORD}" != "" ]
then
  echo " Starting to push Ranger policies as Ranger is selected as a dependency"

  if [ "${HBASE_SERVICE}" != "" ] && [ "${HBASE_SERVICE}" != "none" ]
  then
    echo "Pushing policy to HBase"
    curl -X POST -d "@scripts/policies/hbase.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

  if [ "${HDFS_SERVICE}" != "" ] && [ "${HDFS_SERVICE}" != "none" ]
  then
    echo "Pushing policy to HDFS"
    curl -X POST -d "@scripts/policies/gdfs.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

  if [ "${HIVE_SERVICE}" != "" ] && [ "${HIVE_SERVICE}" != "none" ]
  then
    echo "Pushing policy to Hive"
    curl -X POST -d "@scripts/policies/hive.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

  if [ "${KAFKA_SERVICE}" != "" ] && [ "${KAFKA_SERVICE}" != "none" ]
  then
    echo "Pushing policy to Kafka"
    curl -X POST -d "@scripts/policies/kafka.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

  if [ "${KUDU_SERVICE}" != "" ] && [ "${KUDU_SERVICE}" != "none" ]
  then
    echo "Pushing policy to Kudu"
    curl -X POST -d "@scripts/policies/kudu.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

  if [ "${OZONE_SERVICE}" != "" ] && [ "${OZONE_SERVICE}" != "none" ]
  then
    echo "Pushing policy to Ozone"
    curl -X POST -d "@scripts/policies/ozone.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

  if [ "${SCHEMAREGISTRY_SERVICE}" != "" ] && [ "${SCHEMAREGISTRY_SERVICE}" != "none" ]
  then
    echo "Pushing policy to Schema Registry"
    curl -X POST -d "@scripts/policies/schemaregistry.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

    if [ "${SOLR_SERVICE}" != "" ] && [ "${SOLR_SERVICE}" != "none" ]
    then
      echo "Pushing policy to SolR"
      curl -X POST -d "@scripts/policies/solr.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
    fi

fi


echo "FINISHED INIT OF DATAGEN"
