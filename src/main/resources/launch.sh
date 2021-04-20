#!/usr/bin/env bash

export DIR="/home/root/random-datagen"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via java command"

    hadoop jar random-datagen.jar $@

    sleep 1

echo "*** Finished program ***"