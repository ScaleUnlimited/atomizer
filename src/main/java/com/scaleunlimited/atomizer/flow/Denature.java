package com.scaleunlimited.atomizer.flow;

import com.scaleunlimited.atomizer.parser.BaseParser;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

//Takes in tuples that contain datasetId, recordUuid, and a key/value (attribute name/value) map.
//Applies a parser to each attribute, which generates zero or more Anchors that are saved as the "denatured" output
//
//Question - should we do it this way (with a map), or assume the user of the subassembly will generate separate tuples for each attribute?
//
//Question - does the record ID really need to be a UUID? Or is it sufficient to have it be unique inside of the dataset, so datasetId + recordId == UUID?
//
//Question - do parsers need to operate on an entire record, or should they be passed individual attributes (along with dataset id/record uuid)? Do they need any additional context?
//
//Question - we could try running Clojure and/or Scala code for the parser. Amount of time required is potentially unbounded (e.g. classpath issues) but we could set aside 1-2 days for this.

@SuppressWarnings("serial")
public class Denature extends SubAssembly {

    public Denature(Pipe recordPipe, BaseParser parser) {
        
        Pipe anchorPipe = new Pipe("anchors", recordPipe);
        anchorPipe = new Each(anchorPipe, new ExtractAnchorsFromRecord(parser));
        
        setTails(anchorPipe);
    }
}
