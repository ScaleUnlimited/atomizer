package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;


@SuppressWarnings("serial")
public class AllAtomsDatum extends BaseDatum {

    public static final String ATOM_ID_FN = BaseDatum.fieldName(AllAtomsDatum.class, "atomId");
    public static final String ATOM_FN = BaseDatum.fieldName(AllAtomsDatum.class, "atom");
    public static final String COUNT_FN = BaseDatum.fieldName(AllAtomsDatum.class, "count");

    public static final Fields FIELDS = new Fields(
                    ATOM_ID_FN,
                    ATOM_FN,
                    COUNT_FN);
    
    public AllAtomsDatum(int atomId, String atom, int count) {
        super(FIELDS);
        setAtomId(atomId);
        setAtom(atom);
        setCount(count);
    }
    
    public AllAtomsDatum(Tuple t) {
        super(FIELDS, t);
    }

    public int getAtomId() {
        return _tupleEntry.getInteger(ATOM_ID_FN);
    }
    
    public void setAtomId(int atomId) {
        _tupleEntry.set(ATOM_ID_FN, atomId);
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
