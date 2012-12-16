package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

// AllAtoms is a sub assembly which assigns id numbers to atoms roughly based on frequency of 
// occurrence across all data sets, and keeps the count of each atom.
//
// Generates atomId, atom, count tuples.

@SuppressWarnings("serial")
public class AllAtoms extends SubAssembly {

    public static final String ALLATOMS_PIPE_NAME = "allatoms";


    public AllAtoms(Pipe atomCountPipe) {
        
        Pipe allAtomsPipe = new Pipe(ALLATOMS_PIPE_NAME, atomCountPipe);
        allAtomsPipe = new Each(allAtomsPipe, new AssignAtomId());
        setTails(allAtomsPipe);
    }
    
    
    public Pipe getAllAtomTailPipe() {
        return getTailPipe(ALLATOMS_PIPE_NAME);
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
