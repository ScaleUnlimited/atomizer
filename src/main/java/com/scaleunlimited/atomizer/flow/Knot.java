package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

import com.scaleunlimited.atomizer.datum.AllAtomsDatum;
import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

// Knot is a sub assembly which ties DenaturedAttributes with AllAtoms 
// to generate knotty ties
//
// Generates atomId dataSetId recordId attributeId tuples

@SuppressWarnings("serial")
public class Knot extends SubAssembly {

    public static final String KNOT_PIPE_NAME = "knot";


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
