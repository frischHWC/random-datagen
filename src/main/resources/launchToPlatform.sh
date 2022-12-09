#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#!/usr/bin/env bash

# Set an Hostname with below export to launch it
# export HOST=
# export SSH_KEY="-i <SSH_KEY>"

export USER=root
export DEST_DIR="/root/random-datagen"

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


