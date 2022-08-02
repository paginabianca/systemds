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

# This script runs the federated FTBench T6 and T7 (Crypto data)

# Read Parameters
FILENAME=$0
CMD=${1:-"systemds"}
DATADIR=${2:-"temp/crypto"}
TEMPDIR=${3:-"temp/crypto"}
NUMFED=${4:-2}
DATA=${5:-"${DATADIR}/crypto.csv"}
DATA_BASENAME=$(basename "${DATA}")
BASEPATH=$(dirname "$0")

# Error Prints
err_report(){
  echo "Error in ${FILENAME} on line $1"
}
trap 'err_report $LINENO' ERR

# Set Properties
export SYSDS_QUIET=1
export SYSTEMDS_STANDALONE_OPTS="-Xmx200g -Xms80g -Xmn50g"

# Start the Federated Workers on Localhost
"${BASEPATH}"/utils/startFedWorkers.sh systemds "$TEMPDIR" "$NUMFED" "localhost";

for d in "T6_spec" "T7_spec"
do
  echo "Split And Make Federated"
  ${CMD} -f "${BASEPATH}"/data/splitAndMakeFederatedFrame.dml \
    --config "${BASEPATH}"/../conf/SystemDS-config.xml \
    --nvargs \
      data="${DATA}" \
      nSplit="${NUMFED}" \
      target="${DATADIR}"/"${DATA_BASENAME}"."${d}".fed\
      hosts="${DATADIR}"/workers/hosts \
      fmt="csv"

  # splitting and making the code federated...
  ${CMD} -f "${BASEPATH}"/FTBench/T6T7.dml \
    --config "${BASEPATH}"/../conf/SystemDS-config.xml \
    --nvargs \
      data="${DATADIR}"/"${DATA_BASENAME}"."${d}".fed\
      target="${DATADIR}"/"${DATA_BASENAME}"."${d}".result \
      spec_file="${BASEPATH}"/data/"${d}".json \
      fmt="csv"
done

# Kill the Federated Workers
${BASEPATH}/utils/killFedWorkers.sh $TEMPDIR;
