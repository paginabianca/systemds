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
NUMFED=3
CMD="systemds"
DATADIR=${1:-"~/datasets"}
TEMPDIR=${2:-"~/datasets/temp/T2"}
DATA="${DATADIR}/KDD98.csv"
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

for d in "T2_spec2"
do
  echo "Preprocessing"
  ${CMD} -f "${BASEPATH}"/FTBench/T2.preprocess.dml \
    --config "${BASEPATH}"/../conf/SystemDS-config.xml \
    --nvargs \
      data="${DATA}" \
      target="${TEMPDIR}"/"${DATA_BASENAME}".cleaned \


  echo "Split And Make Federated"
  ${CMD} -f "${BASEPATH}"/data/splitAndMakeFederatedFrame.dml \
    --config "${BASEPATH}"/../conf/SystemDS-config.xml \
    --nvargs \
      data="${TEMPDIR}"/"${DATA_BASENAME}".cleaned \
      nSplit="${NUMFED}" \
      target="${TEMPDIR}"/"${DATA_BASENAME}".${d}.fed \
      hosts="${TEMPDIR}"/workers/hosts \
      fmt="csv"
done
