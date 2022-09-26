#!/bin/bash

echo "STARTING INIT OF DATAGEN"

echo "Create local directory: /tmp/${DATAGEN_USER}"
# Create local directories for data generation
mkdir -p /tmp/${DATAGEN_USER}/
chown datagen:datagen /tmp/${DATAGEN_USER}/
chmod 755 /tmp/${DATAGEN_USER}/

echo "Finished to create local directory"


if [ "${RANGER_SERVICE}" != "" ] && [ "${RANGER_SERVICE}" != "none" ] && [ "${RANGER_URL}" != "" ] && [ "${RANGER_ADMIN_USER}" != "" ] && [ "${RANGER_ADMIN_PASSWORD}" != "" ]
then
  echo " Starting to push Ranger policies as Ranger is selected as a dependency"

  # Push policies to Ranger if Ranger is enabled
  if [ "${HBASE_SERVICE}" != "" ] && [ "${HBASE_SERVICE}" != "none" ]
  then
    echo "Pushing policy to HBase"
    curl -X POST -d "@scripts/policies/hbase.json" -u ${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}  ${RANGER_URL}/service/public/v2/api/policy
  fi

fi


echo "FINISHED INIT OF DATAGEN"
