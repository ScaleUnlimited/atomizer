package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.tuple.Fields;

import com.scaleunlimited.atomizer.datum.AtomCountDatum;
import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

// Takes in tuples from the denatured output (datasetId, recordUuid, anchorId, attributeId, atom)
//
// Counts occurrences of each unique atom value (across all anchors, records, and datasets)
//
// Generates atom, count pairs.

@SuppressWarnings("serial")
public class Atomize extends SubAssembly {

    public static final String ATOM_COUNTS_PIPE_NAME = "atom-counts";

    private static final String ATOM_COUNT_FN = "atom-count";

    public Atomize(Pipe denatureAttributePipe) {
        
        Pipe atomCountsPipe = new Pipe(ATOM_COUNTS_PIPE_NAME, denatureAttributePipe);
        atomCountsPipe = new Each(atomCountsPipe, new Fields(DenaturedAttributeDatum.ATOM_FN), new Identity());
        atomCountsPipe = new CountBy(atomCountsPipe, new Fields(DenaturedAttributeDatum.ATOM_FN), new Fields(ATOM_COUNT_FN));

        Identity identity = new Identity(AtomCountDatum.FIELDS);
        atomCountsPipe = new Each(atomCountsPipe, new Fields(DenaturedAttributeDatum.ATOM_FN, ATOM_COUNT_FN), identity);
        atomCountsPipe = new GroupBy(atomCountsPipe, new Fields(AtomCountDatum.COUNT_FN), true);
        setTails(atomCountsPipe);
    }
    
    
    public Pipe getAtomCountsTailPipe() {
        return getTailPipe(ATOM_COUNTS_PIPE_NAME);
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
