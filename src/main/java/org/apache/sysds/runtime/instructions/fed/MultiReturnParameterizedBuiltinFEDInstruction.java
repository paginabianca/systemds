/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.instructions.fed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.fedplanner.FTypes;
import org.apache.sysds.lops.PickByCount;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.FrameObject;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse.ResponseType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedUDF;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.transform.encode.ColumnEncoderBin;
import org.apache.sysds.runtime.transform.encode.ColumnEncoderComposite;
import org.apache.sysds.runtime.transform.encode.ColumnEncoderRecode;
import org.apache.sysds.runtime.transform.encode.Encoder;
import org.apache.sysds.runtime.transform.encode.EncoderFactory;
import org.apache.sysds.runtime.transform.encode.MultiColumnEncoder;
import org.apache.sysds.runtime.util.IndexRange;

public class MultiReturnParameterizedBuiltinFEDInstruction extends ComputationFEDInstruction {
	private static final Log LOG = LogFactory.getLog(MultiReturnParameterizedBuiltinFEDInstruction.class.getName());
	protected final ArrayList<CPOperand> _outputs;

	private MultiReturnParameterizedBuiltinFEDInstruction(Operator op, CPOperand input1, CPOperand input2,
		ArrayList<CPOperand> outputs, String opcode, String istr) {
		super(FEDType.MultiReturnParameterizedBuiltin, op, input1, input2, null, opcode, istr);
		_outputs = outputs;
	}

	public CPOperand getOutput(int i) {
		return _outputs.get(i);
	}

	public static MultiReturnParameterizedBuiltinFEDInstruction parseInstruction(String str) {
        LOG.debug("parseInstruction");
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		ArrayList<CPOperand> outputs = new ArrayList<>();
		String opcode = parts[0];
        for (int i = 0; i < 4; i++) {
          LOG.debug("parts["+i+"]: " +parts[i]);
        }

		if(opcode.equalsIgnoreCase("transformencode")) {
			// one input and two outputs
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			outputs.add(new CPOperand(parts[3], Types.ValueType.FP64, Types.DataType.MATRIX));
			outputs.add(new CPOperand(parts[4], Types.ValueType.STRING, Types.DataType.FRAME));
			return new MultiReturnParameterizedBuiltinFEDInstruction(null, in1, in2, outputs, opcode, str);
		}
		else {
			throw new DMLRuntimeException("Invalid opcode in MultiReturnBuiltin instruction: " + opcode);
		}

	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// obtain and pin input frame
		FrameObject fin = ec.getFrameObject(input1.getName());
		String spec = ec.getScalarInput(input2).getStringValue();
        Timing timer = new Timing(true);
        Timing t1 = new Timing(false);
        double time = 0.0;

        LOG.debug("processInstruction fin:"+input1.getName()+"\tspec:"+spec);

		String[] colNames = new String[(int) fin.getNumColumns()];
		Arrays.fill(colNames, "");

		// the encoder in which the complete encoding information will be aggregated
		MultiColumnEncoder globalEncoder = new MultiColumnEncoder(new ArrayList<>());
		FederationMap fedMapping = fin.getFedMapping();

		boolean containsEquiWidthEncoder = !fin.isFederated(FTypes.FType.ROW) && spec.toLowerCase().contains("equi-height");
		if(containsEquiWidthEncoder) {
            LOG.debug("containsEquiWidthEncoder");
            t1.start();
			EncoderColnames ret = createGlobalEncoderWithEquiHeight(ec, fin, spec);
            time = t1.stop();
            LOG.debug("createGlobalEncoderWithEquiHeight took: "+time+" ms");

			globalEncoder = ret._encoder;
			colNames = ret._colnames;
		} else {
            LOG.debug("noEquiWidthEncoder");
			// first create encoders at the federated workers, then collect them and aggregate them to a single large
			// encoder
			MultiColumnEncoder finalGlobalEncoder = globalEncoder;
			String[] finalColNames = colNames;
			fedMapping.forEachParallel((range, data) -> {
				int columnOffset = (int) range.getBeginDims()[1];

				// create an encoder with the given spec. The columnOffset (which is 0 based) has to be used to
				// tell the federated worker how much the indexes in the spec have to be offset.
                LOG.debug("about to CreateFrameEncoder. data.getVarID():"+data.getVarID());
                // NOTE: Somewehere Inside this federatedOperation, readExternal is called for a CompositeEncoder
                //       which then creats a bunch of UDFencoders without any name.
				Future<FederatedResponse> responseFuture = data.executeFederatedOperation(new FederatedRequest(
					RequestType.EXEC_UDF,
					-1,
					new CreateFrameEncoder(data.getVarID(), spec, columnOffset + 1)));
				// collect responses with encoders
				try {
					FederatedResponse response = responseFuture.get();
                    LOG.debug("got response from data.executeFederatedOperation");
					MultiColumnEncoder encoder = (MultiColumnEncoder) response.getData()[0];
					// merge this encoder into a composite encoder
					synchronized(finalGlobalEncoder) {
						finalGlobalEncoder.mergeAt(encoder, columnOffset, (int) (range.getBeginDims()[0] + 1));
					}
					// no synchronization necessary since names should anyway match
					String[] subRangeColNames = (String[]) response.getData()[1];
					System.arraycopy(subRangeColNames, 0, finalColNames, (int) range.getBeginDims()[1], subRangeColNames.length);
				}
				catch(Exception e) {
					throw new DMLRuntimeException("Federated encoder creation failed: ", e);
				}
				return null;
			});
			globalEncoder = finalGlobalEncoder;
			colNames = finalColNames;
		}

		// sort for consistent encoding in local and federated
		if(ColumnEncoderRecode.SORT_RECODE_MAP) {
          t1.start();
          globalEncoder.applyToAll(ColumnEncoderRecode.class, ColumnEncoderRecode::sortCPRecodeMaps);
          time = t1.stop();
          LOG.debug("globalEncoder.applyToAll took: "+time+" ms");
		}

		FrameBlock meta = new FrameBlock((int) fin.getNumColumns(), Types.ValueType.STRING);
		meta.setColumnNames(colNames);
		globalEncoder.getMetaData(meta);
		globalEncoder.initMetaData(meta);


        t1.start();
		encodeFederatedFrames(fedMapping, globalEncoder, ec.getMatrixObject(getOutput(0)));
        time = t1.stop();
        LOG.debug("encodeFederatedFrames took: "+time+" ms");

		// release input and outputs
		ec.setFrameOutput(getOutput(1).getName(), meta);
        time = timer.stop();
        LOG.debug("processInstruction took: "+time+" ms");
	}

	private class EncoderColnames {
		public final MultiColumnEncoder _encoder;
		public final String[] _colnames;

		public EncoderColnames(MultiColumnEncoder encoder, String[] colnames) {
			_encoder = encoder;
			_colnames = colnames;
		}
	}

	public EncoderColnames createGlobalEncoderWithEquiHeight(ExecutionContext ec, FrameObject fin, String spec) {
        LOG.debug("createGlobalEncoderWithEquiHeight");
		// the encoder in which the complete encoding information will be aggregated
		MultiColumnEncoder globalEncoder = new MultiColumnEncoder(new ArrayList<>());
		String[] colNames = new String[(int) fin.getNumColumns()];

		Map<Integer, double[]> quantilesPerColumn = new HashMap<>();
		FederationMap fedMapping = fin.getFedMapping();
		fedMapping.forEachParallel((range, data) -> {
			int columnOffset = (int) range.getBeginDims()[1];

			// create an encoder with the given spec. The columnOffset (which is 0 based) has to be used to
			// tell the federated worker how much the indexes in the spec have to be offset.
			Future<FederatedResponse> responseFuture = data.executeFederatedOperation(
				new FederatedRequest(RequestType.EXEC_UDF, -1,
					new CreateFrameEncoder(data.getVarID(), spec, columnOffset + 1)));
			// collect responses with encoders
			try {
				FederatedResponse response = responseFuture.get();
				MultiColumnEncoder encoder = (MultiColumnEncoder) response.getData()[0];

				// put columns to equi-height
				for(Encoder enc : encoder.getColumnEncoders()) {
					if(enc instanceof ColumnEncoderComposite) {
						for(Encoder compositeEncoder : ((ColumnEncoderComposite) enc).getEncoders()) {
							if(compositeEncoder instanceof ColumnEncoderBin && ((ColumnEncoderBin) compositeEncoder).getBinMethod() == ColumnEncoderBin.BinMethod.EQUI_HEIGHT) {
								double quantilrRange = (double) fin.getNumRows() / ((ColumnEncoderBin) compositeEncoder).getNumBin();
								double[] quantiles = new double[((ColumnEncoderBin) compositeEncoder).getNumBin()];
								for(int i = 0; i < quantiles.length; i++) {
									quantiles[i] = quantilrRange * (i + 1);
								}
								quantilesPerColumn.put(((ColumnEncoderBin) compositeEncoder).getColID() + columnOffset - 1, quantiles);
							}
						}
					}
				}

				// merge this encoder into a composite encoder
				synchronized(globalEncoder) {
					globalEncoder.mergeAt(encoder, columnOffset, (int) (range.getBeginDims()[0] + 1));
				}
				// no synchronization necessary since names should anyway match
				String[] subRangeColNames = (String[]) response.getData()[1];
				System.arraycopy(subRangeColNames, 0, colNames, (int) range.getBeginDims()[1], subRangeColNames.length);
			}
			catch(Exception e) {
				throw new DMLRuntimeException("Federated encoder creation failed: ", e);
			}
			return null;
		});

		// calculate all quantiles
		Map<Integer, double[]> equiHeightBinsPerColumn = new HashMap<>();
		for(Map.Entry<Integer, double[]> colQuantiles : quantilesPerColumn.entrySet()) {
			QuantilePickFEDInstruction quantileInstr = new QuantilePickFEDInstruction(
				null, input1, output, PickByCount.OperationTypes.VALUEPICK,true, "qpick", "");
			MatrixBlock quantiles = quantileInstr.getEquiHeightBins(ec, colQuantiles.getKey(), colQuantiles.getValue());
			equiHeightBinsPerColumn.put(colQuantiles.getKey(), quantiles.getDenseBlockValues());
		}

		// modify global encoder
		for(Encoder enc : globalEncoder.getColumnEncoders()) {
			if(enc instanceof ColumnEncoderComposite) {
				for(Encoder compositeEncoder : ((ColumnEncoderComposite) enc).getEncoders())
					if(compositeEncoder instanceof ColumnEncoderBin && ((ColumnEncoderBin) compositeEncoder)
						.getBinMethod() == ColumnEncoderBin.BinMethod.EQUI_HEIGHT)
						((ColumnEncoderBin) compositeEncoder).build(null, equiHeightBinsPerColumn
							.get(((ColumnEncoderBin) compositeEncoder).getColID() - 1));
				((ColumnEncoderComposite) enc).updateAllDCEncoders();
			}
		}
		return new EncoderColnames(globalEncoder, colNames);
	}

	public static void encodeFederatedFrames(FederationMap fedMapping, MultiColumnEncoder globalencoder,
		MatrixObject transformedMat) {
        LOG.debug("encodeFederatedFrames");
        Timing timer = new Timing(true);
		long varID = FederationUtils.getNextFedDataID();
		FederationMap transformedFedMapping = fedMapping.mapParallel(varID, (range, data) -> {
			// copy because we reuse it
            Log LOG = LogFactory.getLog("MultiReturnParameterizedBuiltinFEDInstruction:encodeFederatedFrames:mapParallel");
            Timing t1 = new Timing(true);
            Timing t2 = new Timing(false);
            double time = 0.0;
			long[] beginDims = range.getBeginDims();
			long[] endDims = range.getEndDims();
			IndexRange ixRange = new IndexRange(beginDims[0], endDims[0], beginDims[1], endDims[1]).add(1);// make
																											// 1-based
			IndexRange ixRangeInv = new IndexRange(0, beginDims[0], 0, beginDims[1]);

			// get the encoder segment that is relevant for this federated worker
			MultiColumnEncoder encoder = globalencoder.subRangeEncoder(ixRange);
			// update begin end dims (column part) considering columns added by dummycoding
			encoder.updateIndexRanges(beginDims, endDims, globalencoder.getNumExtraCols(ixRangeInv));

			try {
                t2.start();
				FederatedResponse response = data.executeFederatedOperation(new FederatedRequest(RequestType.EXEC_UDF,
					-1, new ExecuteFrameEncoder(data.getVarID(), varID, encoder))).get();
                time = t2.stop();
                LOG.debug("executeFederatedOperation took:"+time+" ms");
				if(!response.isSuccessful())
					response.throwExceptionFromResponse();
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
            time = t1.stop();
            LOG.debug("mapParallel took:"+time+" ms");
			return null;
		});
        double time = timer.stop();
        LOG.debug("encodeFederatedFrames parallel took:"+time+" ms");

		// construct a federated matrix with the encoded data
		transformedMat.getDataCharacteristics().setDimension(transformedFedMapping.getMaxIndexInRange(0),
			transformedFedMapping.getMaxIndexInRange(1));
		transformedMat.setFedMapping(transformedFedMapping);
        LOG.debug("encodeFederatedFrames done");
	}

	public static class CreateFrameEncoder extends FederatedUDF {
        private static final Log LOG = LogFactory.getLog(CreateFrameEncoder.class.getName());
		private static final long serialVersionUID = 2376756757742169692L;
		private final String _spec;
		private final int _offset;

		public CreateFrameEncoder(long input, String spec, int offset) {
			super(new long[] {input});
			_spec = spec;
			_offset = offset;
		}

		@Override
		public FederatedResponse execute(ExecutionContext ec, Data... data) {
            LOG.debug("executing...");
			FrameObject fo = (FrameObject) data[0];
			FrameBlock fb = fo.acquireRead();
			String[] colNames = fb.getColumnNames();

			// create the encoder
            LOG.debug("Right before EncoderFactory.createEncoder. Maybe pass the EC through the Contstructor?");
			MultiColumnEncoder encoder = EncoderFactory
				.createEncoder(_spec, colNames, fb.getNumColumns(), null, _offset, _offset + fb.getNumColumns());

			// build necessary structures for encoding
            // NOTE: in this build of the MultiColumnEncoder there are a bunch of
            Timing t1 = new Timing(true);
            if(ConfigurationManager.getDMLConfig().getBooleanValue(DMLConfig.FEDERATED_PAR_TRANSFORMENCODE)){
              LOG.info("building with " +OptimizerUtils.getTransformNumThreads() +" threads");
              encoder.build(fb, OptimizerUtils.getTransformNumThreads() ); // FIXME skip equi-height sorting
            } else{
              LOG.info("building with 1 threads");
              encoder.build(fb);
            }
            // encoder.build(fb, InfrastructureAnalyzer.getLocalParallelism() ); // FIXME skip equi-height sorting
            double time = t1.stop();
            LOG.info("Build took: "+ time +"ms");
			fo.release();

			// create federated response
			return new FederatedResponse(ResponseType.SUCCESS, new Object[] {encoder, fb.getColumnNames()});
		}

		@Override
		public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			return null;
		}
	}

	public static class ExecuteFrameEncoder extends FederatedUDF {
		private static final long serialVersionUID = 6034440964680578276L;
		private final long _outputID;
		private final MultiColumnEncoder _encoder;

		public ExecuteFrameEncoder(long input, long output, MultiColumnEncoder encoder) {
			super(new long[] {input});
            Timing t1 = new Timing(true);
			_outputID = output;
			_encoder = encoder;
            double time = t1.stop();
            LOG.debug("ExecuteFrameEncoder Contstructor took: " +time + " ms");
		}

		@Override
		public FederatedResponse execute(ExecutionContext ec, Data... data) {
          Timing timer = new Timing(true);
          Timing t1 = new Timing(false);
          double time  = 0.0;
            LOG.debug("executing ExecuteFrameEncoder");
          // String funcName = ec.getScalarInput(inputs[0]).getStringValue();

            t1.start();
			FrameBlock fb = ((FrameObject) data[0]).acquireReadAndRelease();
            time = t1.stop();
            LOG.debug("acquireReadAndRelease: took: " + time + " ms");

			// offset is applied on the Worker to shift the local encoders to their respective column
            t1.start();
			_encoder.applyColumnOffset();
            time = t1.stop();
            LOG.debug("applyColumnOffset: took: " + time + " ms");
			// apply transformation
            t1.start();
			MatrixBlock mbout = _encoder.apply(fb);
            time = t1.stop();
            LOG.debug("applied encodeer ExecuteFrameEncoder: took: " + time + " ms");

			// create output matrix object
			MatrixObject mo = ExecutionContext.createMatrixObject(mbout);

			// add it to the list of variables
			ec.setVariable(String.valueOf(_outputID), mo);

			// return id handle
            time = timer.stop();
            LOG.debug("execute took:" + time + " ms");
			return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
		}

		@Override
		public List<Long> getOutputIds() {
			return new ArrayList<>(Arrays.asList(_outputID));
		}

		@Override
		public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			LineageItem[] liUdfInputs = Arrays.stream(getInputIDs())
				.mapToObj(id -> ec.getLineage().get(String.valueOf(id))).toArray(LineageItem[]::new);
			// calculate checksum for the encoder
			Checksum checksum = new Adler32();
			byte[] bytes = SerializationUtils.serialize(_encoder);
			checksum.update(bytes, 0, bytes.length);
			CPOperand encoder = new CPOperand(String.valueOf(checksum.getValue()), ValueType.INT64, DataType.SCALAR,
				true);
			LineageItem[] otherInputs = LineageItemUtils.getLineage(ec, encoder);
			LineageItem[] liInputs = Stream.concat(Arrays.stream(liUdfInputs), Arrays.stream(otherInputs))
				.toArray(LineageItem[]::new);
			return Pair.of(String.valueOf(_outputID), new LineageItem(getClass().getSimpleName(), liInputs));
		}
	}
}
