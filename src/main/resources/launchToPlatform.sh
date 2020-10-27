#!/usr/bin/env bash

# Set an Hostname with below export to launch it
# export HOST=

export USER=root
export DEST_DIR="/home/root/random-datagen"

echo "Create needed directory on platform and send required files there"

ssh ${USER}@${HOST} "mkdir -p ${DEST_DIR}/"
ssh ${USER}@${HOST} "mkdir -p ${DEST_DIR}/resources/"

scp src/main/resources/models/*.json ${USER}@${HOST}:${DEST_DIR}/
scp src/main/resources/dictionnaries/* ${USER}@${HOST}:${DEST_DIR}/
scp src/main/resources/*.properties ${USER}@${HOST}:${DEST_DIR}/
scp src/main/resources/launch.sh ${USER}@${HOST}:${DEST_DIR}/

ssh ${USER}@${HOST} "chmod +x ${DEST_DIR}/launch.sh"

scp target/random-datagen-*.jar ${USER}@${HOST}:${DEST_DIR}/random-datagen.jar

echo "Finished to send required files"

echo "Launch script on platform to launch program properly"
ssh ${USER}@${HOST} 'bash -s' < src/main/resources/launch.sh $@
echo "Program finished"


