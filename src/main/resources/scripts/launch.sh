#!/usr/bin/env bash

export DIR="/root/random-datagen"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via java command"

    export JAVA_HOME=/usr/lib/jvm/java-11/
    ${JAVA_HOME}/bin/java -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED -Xmx16G -jar random-datagen.jar $@

    sleep 1

echo "*** Finished program ***"