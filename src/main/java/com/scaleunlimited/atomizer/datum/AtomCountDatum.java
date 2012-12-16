package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;


@SuppressWarnings("serial")
public class AtomCountDatum extends BaseDatum {

    public static final String ATOM_FN = BaseDatum.fieldName(AtomCountDatum.class, "atom");
    public static final String COUNT_FN = BaseDatum.fieldName(AtomCountDatum.class, "count");

    public static final Fields FIELDS = new Fields(
                    ATOM_FN,
                    COUNT_FN);
    
    public AtomCountDatum(String atom, int count) {
        super(FIELDS);
        setAtom(atom);
        setCount(count);
    }
    
    public AtomCountDatum(Tuple t) {
        super(FIELDS, t);
    }

    public String getAtom() {
        return _tupleEntry.getString(ATOM_FN);
    }
    
    public void setAtom(String atom) {
        _tupleEntry.set(ATOM_FN, atom);
    }

    public int getCount() {
        return _tupleEntry.getInteger(COUNT_FN);
    }

    public void setCount(int count) {
        _tupleEntry.set(COUNT_FN, count);
    }

}
