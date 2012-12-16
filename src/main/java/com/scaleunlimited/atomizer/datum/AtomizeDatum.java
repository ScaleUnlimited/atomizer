package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;


@SuppressWarnings("serial")
public class AtomizeDatum extends BaseDatum {

    public static final String ATOM_FN = BaseDatum.fieldName(AtomizeDatum.class, "atom");
    public static final String COUNT_FN = BaseDatum.fieldName(AtomizeDatum.class, "count");
    public static final String TUPLE_NUMBER_FN = BaseDatum.fieldName(AtomizeDatum.class, "tupleNumber");

    public static final Fields FIELDS = new Fields(
                    ATOM_FN,
                    COUNT_FN,
                    TUPLE_NUMBER_FN);
    
    public AtomizeDatum(String atom, int count, int tupleNumber) {
        super(FIELDS);
        setAtom(atom);
        setCount(count);
        setTupleNumber(tupleNumber);
   }
    
    public AtomizeDatum(Tuple t) {
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

    public int getTupleNumber() {
        return _tupleEntry.getInteger(TUPLE_NUMBER_FN);
    }

    public void setTupleNumber(int tupleNumber) {
        _tupleEntry.set(TUPLE_NUMBER_FN, tupleNumber);
    }

}
