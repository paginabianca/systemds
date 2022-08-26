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

# This script runs the federated FTBench T5 (santander transaction prediction)

# Read Parameters
FILENAME=$0
CMD=${1:-"systemds"}
DATADIR=${2:-"temp/santander"}
TEMPDIR=${3:-"temp/T5"}
NUMFED=${4:-2}
DATA=${5:-"${DATADIR}/santander.csv"}
DATA_BASENAME=$(basename "${DATA}")
BASEPATH=$(dirname "$0")
CONFIG_FILE=${6:-"../conf/SystemDS-config.xml"}

# Error Prints
err_report(){
  echo "Error in ${FILENAME} on line $1"
}
trap 'err_report $LINENO' ERR

# Set Properties
export SYSDS_QUIET=0
export LOG4JPROP=${BASEPATH}/../conf/log4j.properties
export SYSTEMDS_STANDALONE_OPTS="-Xmx110g -Xms50g -Xmn20g"

# Create Temp Directory
if [ ! -d ${TEMPDIR} ]; then
  mkdir -p ${TEMPDIR}
fi

# Start the Federated Workers on Localhost
"${BASEPATH}"/utils/startFedWorkers.sh systemds "${TEMPDIR}" "$NUMFED" "localhost" "${CONFIG_FILE}";

# santander_spec1 equi-with binning with numbins=10
for d in "T5_spec"
do
  echo "Split And Make Federated"
  ${CMD} -f "${BASEPATH}"/data/splitAndMakeFederatedFrame.dml \
    --config "${BASEPATH}"/../conf/SystemDS-config.xml \
    --nvargs \
      data="${DATA}" \
      nSplit="${NUMFED}" \
      target="${TEMPDIR}"/"${DATA_BASENAME}".fed\
      hosts="${TEMPDIR}"/workers/hosts \
      fmt="csv"

  echo "FTBench"
  ${CMD} -f "${BASEPATH}"/FTBench/T5.dml \
    --config "${CONFIG_FILE}" \
    --nvargs \
      data="${TEMPDIR}"/"${DATA_BASENAME}".fed\
      target="${TEMPDIR}"/"${DATA_BASENAME}"."${d}".result \
      spec_file="${BASEPATH}"/data/"${d}".json \
      fmt="csv"
done

# Kill the Federated Workers
"${BASEPATH}"/utils/killFedWorkers.sh "$TEMPDIR";