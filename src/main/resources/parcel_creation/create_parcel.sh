#!/usr/bin/env bash
CDP_VERSION="7.1.7.1000"
RD_VERSION="0.1.1"
TEMP_DIR="/tmp/datagen_parcel_files"
TEMP_PARCEL_DIR="/tmp/datagen_parcel"
DISTRO_SUFFIX="el7"

mkdir -p "${TEMP_DIR}"
mkdir -p "${TEMP_PARCEL_DIR}"

export PARCEL_VERSION="${RD_VERSION}.${CDP_VERSION}"
export PARCEL_NAME="DATAGEN-${PARCEL_VERSION}"
PARCEL_DIR="${TEMP_DIR}/${PARCEL_NAME}"

rm -rf "${PARCEL_DIR}"
rm -rf "${TEMP_DIR}/${PARCEL_NAME}.parcel"
mkdir -p "${PARCEL_DIR}"
mkdir -p "${PARCEL_DIR}/meta/"
mkdir -p "${PARCEL_DIR}/models/"
mkdir -p "${PARCEL_DIR}/dictionaries/"

# Place all needed files in a temp_dir with right structure
cp parcel.json "${PARCEL_DIR}/meta/"
cp release-notes.txt "${PARCEL_DIR}/meta/"
cp datagen_env.sh "${PARCEL_DIR}/meta/"
cp ../dictionaries/* "${PARCEL_DIR}/dictionaries/"
cp ../models/* "${PARCEL_DIR}/models/"
cp ../logback.xml "${PARCEL_DIR}/"
cp ../application.properties "${PARCEL_DIR}/"
cp ../../../../target/random-datagen*.jar "${PARCEL_DIR}/${PARCEL_NAME}.jar"

envsubst < "${PARCEL_DIR}/meta/parcel.json" > "${PARCEL_DIR}/meta/parcel.json.tmp" \
  && mv "${PARCEL_DIR}/meta/parcel.json.tmp" "${PARCEL_DIR}/meta/parcel.json"

envsubst < "${PARCEL_DIR}/meta/release-notes.txt" > "${PARCEL_DIR}/meta/release-notes.txt.tmp" \
  && mv "${PARCEL_DIR}/meta/release-notes.txt.tmp" "${PARCEL_DIR}/meta/release-notes.txt"

# Tar all this tmp dir in one with owner/group being root
cd ${TEMP_DIR}
rm -rf ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel
tar -czvf ${TEMP_PARCEL_DIR}/${PARCEL_NAME}-${DISTRO_SUFFIX}.parcel ${PARCEL_NAME}/
