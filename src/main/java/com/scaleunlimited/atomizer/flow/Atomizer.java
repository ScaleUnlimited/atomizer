package com.scaleunlimited.atomizer.flow;

import com.scaleunlimited.atomizer.datum.AnchorDatum;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.tuple.Fields;

// Takes in tuples from the denatured output (datasetId, recordUuid, anchorId, attribute name, atom, confidence level)
//
// Counts occurrences of each unique atom value (across all anchors, records, and datasets)
//
// Generates atom, count pairs.

@SuppressWarnings("serial")
public class Atomizer extends SubAssembly {

    private static final String ATOM_COUNT_FN = "atom-count";

    public Atomizer(Pipe anchorPipe) {
        
        Pipe atomPipe = new Pipe("atoms", anchorPipe);
        atomPipe = new Each(atomPipe, new Fields(AnchorDatum.ATOM_FN), new Identity());
        atomPipe = new CountBy(atomPipe, new Fields(AnchorDatum.ATOM_FN), new Fields(ATOM_COUNT_FN));
        
        setTails(atomPipe);
    }
}
