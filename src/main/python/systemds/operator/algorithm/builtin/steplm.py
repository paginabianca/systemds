# -------------------------------------------------------------
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
# -------------------------------------------------------------

# Autogenerated By   : src/main/python/generator/generator.py
# Autogenerated From : scripts/builtin/steplm.dml

from typing import Dict, Iterable

from systemds.operator import OperationNode, Matrix, Frame, List, MultiReturn, Scalar
from systemds.script_building.dag import OutputType
from systemds.utils.consts import VALID_INPUT_TYPES


def steplm(X: Matrix,
           y: Matrix,
           **kwargs: Dict[str, VALID_INPUT_TYPES]):
    """
     The steplm-function (stepwise linear regression) implements a classical forward feature selection method.
     This method iteratively runs what-if scenarios and greedily selects the next best feature
     until the Akaike information criterion (AIC) does not improve anymore. Each configuration trains a regression model
     via lm, which in turn calls either the closed form lmDS or iterative lmGC.
    
     .. code-block:: txt 
    
       return: Matrix of regression parameters (the betas) and its size depend on icpt input value:
               OUTPUT SIZE:   OUTPUT CONTENTS:                HOW TO PREDICT Y FROM X AND B:
       icpt=0: ncol(X)   x 1  Betas for X only                Y ~ X %*% B[1:ncol(X), 1], or just X %*% B
       icpt=1: ncol(X)+1 x 1  Betas for X and intercept       Y ~ X %*% B[1:ncol(X), 1] + B[ncol(X)+1, 1]
       icpt=2: ncol(X)+1 x 2  Col.1: betas for X & intercept  Y ~ X %*% B[1:ncol(X), 1] + B[ncol(X)+1, 1]
                              Col.2: betas for shifted/rescaled X and intercept
    
     In addition, in the last run of linear regression some statistics are provided in CSV format, one comma-separated
     name-value pair per each line, as follows:
    
    
    
    :param X: Location (on HDFS) to read the matrix X of feature vectors
    :param Y: Location (on HDFS) to read the 1-column matrix Y of response values
    :param icpt: Intercept presence, shifting and rescaling the columns of X:
        0 = no intercept, no shifting, no rescaling;
        1 = add intercept, but neither shift nor rescale X;
        2 = add intercept, shift & rescale X columns to mean = 0, variance = 1
    :param reg: learning rate
    :param tol: Tolerance threshold to train until achieved
    :param maxi: maximum iterations 0 means until tolerance is reached
    :param verbose: If the algorithm should be verbose
    :return: Matrix of regression parameters (the betas) and its size depend on icpt input value.
    :return: Matrix of selected features ordered as computed by the algorithm.
    """

    params_dict = {'X': X, 'y': y}
    params_dict.update(kwargs)
    
    vX_0 = Matrix(X.sds_context, '')
    vX_1 = Matrix(X.sds_context, '')
    output_nodes = [vX_0, vX_1, ]

    op = MultiReturn(X.sds_context, 'steplm', output_nodes, named_input_nodes=params_dict)

    vX_0._unnamed_input_nodes = [op]
    vX_1._unnamed_input_nodes = [op]

    return op
