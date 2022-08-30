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

# Can be used to generate hosts metadata file to be used in
#   - ./splitAndMakeFederated.sh
#   - ./splitAndMakeFederatedFrame.sh
# as the 'hosts' argument.
#
# Arguments:
#   1. Output directory
#
# Example:
#   ./genHostData.sh ~/my/hosts/file
#
# This example will first remove the following files if existent:
#   -  ~/my/hosts/file/hosts <- a directory
#   -  ~/my/hosts/file/hosts.mtd <- a file

# Edit the hosts where your workers are running.
declare -a HOSTS=("129.27.206.6:8022" "129.27.206.14:8022")

OUT=${1:-"~/datasets/temp"}
HOSTS_META="{\"data_type\": \"list\", \"rows\": ${#HOSTS[@]}, \"cols\": 1, \"format\": \"text\"}"
HOST_COUNT=${#HOSTS[@]}
HOST_META="{\"data_type\": \"scalar\", \"value_type\": \"string\", \"format\": \"text\"}"


if [ -d "${OUT}/hosts" ]; then
  rm -rf  "${OUT}/hosts"
fi

if [ -f "${OUT}/hosts.mtd" ]; then
  rm -rf  "${OUT}/hosts.mtd"
fi

mkdir -p "${OUT}/hosts"
echo "${HOSTS_META}" > "${OUT}"/hosts.mtd

for (( i=0; i<"${HOST_COUNT}"; i++)); do
  echo "${HOSTS[$i]}" > "${OUT}/hosts/${i}_null"
  echo "${HOST_META}" > "${OUT}/hosts/${i}_null.mtd"
done
