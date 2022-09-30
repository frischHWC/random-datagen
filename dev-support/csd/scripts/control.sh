#!/bin/bash
CMD=$1

case $CMD in
  (start)
    echo "Starting DATAGEN"
    envsubst < "${CONF_DIR}/service.properties" > "${CONF_DIR}/service.properties.tmp"
    mv "${CONF_DIR}/service.properties.tmp" "${CONF_DIR}/service.properties"
    chmod 700 "${CONF_DIR}/service.properties"
    exec ${JAVA_HOME}/bin/java -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED -Dspring.profiles.active=cdp -Dserver.port=${SERVER_PORT} -Dsolr.ssl.checkPeerName=false -jar -Xmx${MAX_HEAP_SIZE}G ${DATAGEN_JAR_PATH} --spring.config.location=file:${CONF_DIR}/service.properties
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac