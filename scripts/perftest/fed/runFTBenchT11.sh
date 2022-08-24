#!/bin/bash
#-------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------

# Read Parameters
FILENAME=$0
CMD=${1:-"systemds"}
DATADIR=${2:-"temp/T11"}
TEMPDIR=${3:-"temp/T11"}
NUMFED=${4:-2}
CONFIG_FILE=${5:-"../conf/SystemDS-config.xml"}
DATA=${6:-"${DATADIR}/AminerAbstractSequence.csv"}
METAFRAME=${7:-"${DATADIR}/wiki_embeddings/wiki_metaframe"}
EMBEDDINGS=${8:-"${DATADIR}/wiki_embeddings/wiki_embeddings"}
DATA_BASENAME=$(basename "${DATA}")
BASEPATH=$(dirname "$0")

# Error Prints
err_report(){
  echo "Error in ${FILENAME} on line $1"
}
trap 'err_report $LINENO' ERR

# Set Properties
export SYSDS_QUIET=1
export LOG4JPROP=${BASEPATH}'/../conf/log4j.properties'
export SYSTEMDS_STANDALONE_OPTS="-Xmx120g -Xms80g -Xmn50g"

# Create Temp Directory
if [ ! -d ${TEMPDIR} ]; then
  mkdir -p ${TEMPDIR}
fi

# Start the Federated Workers on Localhost
"${BASEPATH}"/utils/startFedWorkers.sh systemds "${TEMPDIR}" "${NUMFED}" "localhost" "${CONFIG_FILE}";


for d in "T11"
do
  for INPUT in $METAFRAME #$DATA #$EMBEDDINGS -- only metaframe is needed since it's the only one that fails
  do
    echo "Split And Make Federated "$INPUT
    ${CMD} -f "${BASEPATH}"/data/splitAndMakeFederatedFrame.dml \
      --config "${BASEPATH}"/../conf/SystemDS-config.xml \
      --nvargs \
        data="${INPUT}" \
        nSplit="${NUMFED}" \
        target="${TEMPDIR}"/"$(basename "$INPUT")".${d}.fed \
        hosts="${TEMPDIR}"/workers/hosts \
        fmt="csv"
  done
  for INPUT in $EMBEDDINGS
  do
    echo "Split And Make Federated "$INPUT
    ${CMD} -f "${BASEPATH}"/data/splitAndMakeFederated.dml \
      --config "${BASEPATH}"/../conf/SystemDS-config.xml \
      --nvargs \
        data="${INPUT}" \
        nSplit="${NUMFED}" \
        target="${TEMPDIR}"/"$(basename "$INPUT")".${d}.fed \
        hosts="${TEMPDIR}"/workers/hosts \
        fmt="csv"
  done

  echo "FTBench"
  ${CMD} -f "${BASEPATH}"/FTBench/T11.dml \
    --config "${CONFIG_FILE}" \
    --nvargs \
      data="${TEMPDIR}"/"${DATA_BASENAME}".${d}.fed \
      metaframe="${TEMPDIR}"/"$(basename "$METAFRAME")".${d}.fed \
      embeddings="${TEMPDIR}"/"$(basename "$EMBEDDINGS")".${d}.fed \
      target="${TEMPDIR}"/"$(basename ${CONFIG_FILE})".${d}.result \
      spec_file="${BASEPATH}"/data/${d}.json \
      fmt="csv"
done

# Kill the Federated Workers
"${BASEPATH}"/utils/killFedWorkers.sh "${TEMPDIR}"
