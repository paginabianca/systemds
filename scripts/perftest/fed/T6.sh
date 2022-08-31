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

# This branch is abused as cloud-save.

# quickly run the T2 FTBench on workers that already have been
# populated with the federated data.

# Read Parameters
FILENAME=$0
CMD=${1:-"systemds"}
DATADIR=${2:-"~/datasets"}
TEMPDIR=${3:-"~/datasets/temp/T6"}
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
export LOG4JPROP=${BASEPATH}'/../conf/log4j.properties'
export SYSTEMDS_STANDALONE_OPTS="-Xmx120g -Xms80g -Xmn50g"

# Create Temp Directory
if [ ! -d ${TEMPDIR} ]; then
  mkdir -p ${TEMPDIR}
fi

for d in "T6_spec"
do
  echo "FTBench"
  ${CMD} -f "${BASEPATH}"/FTBench/T6.dml \
    --config "${BASEPATH}"/../conf/SystemDS-config.xml \
    --nvargs \
      data="${TEMPDIR}"/"${DATA_BASENAME}".${d}.fed \
      target="${TEMPDIR}"/"${DATA_BASENAME}".${d}.result \
      spec_file="${BASEPATH}"/data/${d}.json \
      fmt="csv"
done
