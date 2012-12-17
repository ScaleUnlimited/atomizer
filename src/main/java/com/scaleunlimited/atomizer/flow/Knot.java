package com.scaleunlimited.atomizer.flow;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.atomizer.datum.AllAtomsDatum;
import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.atomizer.datum.KnotDatum;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;

// Knot is a sub assembly which ties DenaturedAttributes with AllAtoms 
// to generate knotty ties
//
// Generates atomId dataSetId recordId attributeId tuples

@SuppressWarnings("serial")
public class Knot extends AtomizerSubAssembly {

    public static final String KNOT_PIPE_NAME = "knot";

    @SuppressWarnings({"rawtypes"})
    public static class CreateKnottyTie extends BaseOperation<NullContext> implements Function<NullContext> {

        private static final Logger LOGGER = Logger.getLogger(CreateKnottyTie.class);

        private transient LoggingFlowProcess _flowProcess;

        public CreateKnottyTie() {
            super(KnotDatum.FIELDS);
            
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            LOGGER.info("Starting CreateKnottyTie");
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
        }

        @Override
        public void cleanup(final FlowProcess flowProcess, final OperationCall<NullContext> operationCall) {
            super.cleanup(flowProcess, operationCall);
            LOGGER.info("Ending CreateKnottyTie");
            _flowProcess.dumpCounters();
        }


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            TupleEntryCollector outputCollector = functionCall.getOutputCollector();
            TupleEntry te = functionCall.getArguments();
            DenaturedAttributeDatum denaturedDatum = new DenaturedAttributeDatum(TupleEntry.select(DenaturedAttributeDatum.FIELDS, te));
            AllAtomsDatum allAtomsDatum = new AllAtomsDatum(TupleEntry.select(AllAtomsDatum.FIELDS, te));

            KnotDatum output = new KnotDatum(allAtomsDatum.getAtomId(), denaturedDatum.getDatasetId(), 
                            denaturedDatum.getRecordUuid(), denaturedDatum.getAttributeId());

            outputCollector.add(output.getTuple());
            flowProcess.increment(AtomizerCounters.TOTAL_KNOTS, 1);
        }
    }

    public Knot(Pipe denaturedAttributesPipe, Pipe allAtomsPipe) {
        
        // Join on the atom 
        // TODO VMa - is the lhs small enough for memory ? Or should we just use a CoGroup ?
        Pipe hashPipe = new HashJoin(denaturedAttributesPipe, new Fields(DenaturedAttributeDatum.ATOM_FN), 
                        allAtomsPipe, new Fields(AllAtomsDatum.ATOM_FN));
        Pipe knotPipe = new Pipe(KNOT_PIPE_NAME, hashPipe);
        knotPipe = new Each(knotPipe, new CreateKnottyTie(), Fields.RESULTS);
        setTails(knotPipe);
    }
    
    
    public Pipe getKnotTailPipe() {
        return getTailPipe(KNOT_PIPE_NAME);
    }
    
}
