#!/bin/bash
CMD=$1

case $CMD in
  (start)
    echo "Starting DATAGEN"
    echo $CONF_DIR
    echo $(pwd)
    envsubst < "${CONF_DIR}/service.properties" > "${CONF_DIR}/service.properties.tmp"
    mv "${CONF_DIR}/service.properties.tmp" "${CONF_DIR}/service.properties"
    exec ${JAVA_HOME}/bin/java -jar -Dspring.profiles.active=cdp -Xmx${MAX_HEAP_SIZE}G ${DATAGEN_JAR_PATH}
    ;;
 (local_json)
     echo "Generates JSON Data Locally"
     exit 0
     ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac