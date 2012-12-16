package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.atomizer.datum.AtomizeDatum;
import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;

// Takes in tuples from the denatured output (datasetId, recordUuid, anchorId, attributeId, atom)
//
// Counts occurrences of each unique atom value (across all anchors, records, and datasets)
//
// Generates atom, count pairs.

@SuppressWarnings("serial")
public class Atomize extends SubAssembly {

    public static final String ATOMIZE_PIPE_NAME = "atomize";

    private static final String ATOM_COUNT_FN = "atom-count";

    
    @SuppressWarnings({"rawtypes"})
    private static class CreateAtomizeDatum extends BaseOperation<NullContext> implements Function<NullContext> {

        private static final Logger LOGGER = Logger.getLogger(CreateAtomizeDatum.class);

        private transient LoggingFlowProcess _flowProcess;
        private transient int _tupleNumber;


        public CreateAtomizeDatum() {
            super(AtomizeDatum.FIELDS);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            LOGGER.info("Starting CreateAtomizeDatum");
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
            _tupleNumber = 0; 
            
        }

        @Override
        public void cleanup(final FlowProcess flowProcess, final OperationCall<NullContext> operationCall) {
            super.cleanup(flowProcess, operationCall);
            LOGGER.info("Ending CreateAtomizeDatum");
            _flowProcess.dumpCounters();
        }


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            TupleEntryCollector outputCollector = functionCall.getOutputCollector();
            TupleEntry te = functionCall.getArguments();
            
            AtomizeDatum output = new AtomizeDatum(te.getString(DenaturedAttributeDatum.ATOM_FN), 
                            te.getInteger(ATOM_COUNT_FN), 
                            _tupleNumber);
            outputCollector.add(output.getTuple());
            _flowProcess.increment(AtomizerCounters.TOTAL_ATOMIZE, 1);
            _tupleNumber++;
        }
    }

    public Atomize(Pipe denatureAttributePipe) {
        
        Pipe atomCountsPipe = new Pipe("atom counts", denatureAttributePipe);
        atomCountsPipe = new Each(atomCountsPipe, new Fields(DenaturedAttributeDatum.ATOM_FN), new Identity());
        atomCountsPipe = new CountBy(atomCountsPipe, new Fields(DenaturedAttributeDatum.ATOM_FN), new Fields(ATOM_COUNT_FN));

        Pipe atomizePipe = new Pipe(ATOMIZE_PIPE_NAME, atomCountsPipe);
        atomizePipe = new Each(atomizePipe, new CreateAtomizeDatum());

        // TODO VMa - why do we need to group by the atom count ?
//        atomCountsPipe = new GroupBy(atomCountsPipe, new Fields(AtomCountDatum.COUNT_FN), true);
        setTails(atomizePipe);
    }
    
    
    public Pipe getAtomizeTailPipe() {
        return getTailPipe(ATOMIZE_PIPE_NAME);
    }
    
    private Pipe getTailPipe(String pipeName) {
        String[] pipeNames = getTailNames();
        for (int i = 0; i < pipeNames.length; i++) {
            if (pipeName.equals(pipeNames[i])) {
                return getTails()[i];
            }
        }
        throw new InvalidParameterException("Invalid pipe name: " + pipeName);
    }

}
