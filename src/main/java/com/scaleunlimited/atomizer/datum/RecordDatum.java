package com.scaleunlimited.atomizer.datum;

import java.util.HashMap;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;

// record has datasetId, recordUuid, and a key/value (attribute name/value) map

@SuppressWarnings("serial")
public class RecordDatum extends BaseDatum {

    public static final String DATASET_ID_FN = BaseDatum.fieldName(RecordDatum.class, "datasetId");
    public static final String RECORD_UUID_FN = BaseDatum.fieldName(RecordDatum.class, "recordUuid");
    public static final String ATTRIBUTE_MAP_FN = BaseDatum.fieldName(RecordDatum.class, "attributeMap");

    public static final Fields FIELDS = new Fields(
                    DATASET_ID_FN,
                    RECORD_UUID_FN,
                    ATTRIBUTE_MAP_FN);
    
    public RecordDatum(String datasetId, String recordUuid, Map<String, String>attributeMap) {
        super(FIELDS);
        setDatasetId(datasetId);
        setRecordUuid(recordUuid);
        setAttributeMap(attributeMap);
    }
    
    public RecordDatum(Tuple t) {
        super(FIELDS, t);
    }

    public String getDatasetId() {
        return _tupleEntry.getString(DATASET_ID_FN);
    }
    
    public void setDatasetId(String datasetId) {
        _tupleEntry.set(DATASET_ID_FN, datasetId);
    }

    public String getRecordUuid() {
        return _tupleEntry.getString(RECORD_UUID_FN);
    }
    
    public void setRecordUuid(String recordUuid) {
        _tupleEntry.set(RECORD_UUID_FN, recordUuid);
    }
    
    public Map<String,String> getAttributeMap() {
        return tupleToMap((Tuple)(_tupleEntry.getObject(ATTRIBUTE_MAP_FN)));
    }
    
    public void setAttributeMap(Map<String, String> attributeMap) {
        _tupleEntry.set(ATTRIBUTE_MAP_FN, mapToTuple(attributeMap));
    }

    private static Tuple mapToTuple(Map<String, ?> map) {
        if (map == null) {
            return null;
        }
        
        Tuple result = new Tuple();
        for (Map.Entry<String,?> entry : map.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue());
        }
        return result;
    }

    private Map<String, String> tupleToMap(Tuple tuple) {
        Map<String, String> result = new HashMap<String, String>();
        if (tuple != null) {
            int numHeaders = tuple.size() / 2;
            for (int i = 0; i < numHeaders; i++) {
                result.put(tuple.getString(i*2), tuple.getString((i*2)+1));
            }
        }
        
        return result;
    }

}
