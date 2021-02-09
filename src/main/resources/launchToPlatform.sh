#!/usr/bin/env bash

# Set an Hostname with below export to launch it
# export HOST=
# export SSH_KEY="-i <SSH_KEY>"

export USER=root
export DEST_DIR="/home/root/random-datagen"

echo "Create needed directory on platform and send required files there"

ssh ${SSH_KEY} ${USER}@${HOST} "mkdir -p ${DEST_DIR}/"
ssh ${SSH_KEY} ${USER}@${HOST} "mkdir -p ${DEST_DIR}/resources/"

scp ${SSH_KEY} src/main/resources/models/*.json ${USER}@${HOST}:${DEST_DIR}/
scp ${SSH_KEY} src/main/resources/dictionnaries/* ${USER}@${HOST}:${DEST_DIR}/
scp ${SSH_KEY} src/main/resources/*.properties ${USER}@${HOST}:${DEST_DIR}/
scp ${SSH_KEY} src/main/resources/launch.sh ${USER}@${HOST}:${DEST_DIR}/

ssh ${SSH_KEY} ${USER}@${HOST} "chmod +x ${DEST_DIR}/launch.sh"

scp ${SSH_KEY} target/random-datagen-*.jar ${USER}@${HOST}:${DEST_DIR}/random-datagen.jar

echo "Finished to send required files"

echo "Launch script on platform to launch program properly"
ssh ${SSH_KEY} ${USER}@${HOST} 'bash -s' < src/main/resources/launch.sh $@
echo "Program finished"


