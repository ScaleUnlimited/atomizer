package com.scaleunlimited.atomizer.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

public class SimpleExtractor extends BaseExtractor {

    private static final Logger LOGGER = Logger.getLogger(SimpleExtractor.class);

    private Map<String, String> _attributeNameToIdMap;
    private Map<String, String> _attributeIdToAnchorIdMap;
    private Map<String, Set<String>> _datasetIdToAttributeSetMap;


    public SimpleExtractor(Map<String, String>attributeNameToIdMap, Map<String, String>attributeIdToAnchorIdMap,
                    Map<String, String>datasetIdToMetaRecord, List<String>metaRecords) {
        _attributeNameToIdMap = attributeNameToIdMap;
        _attributeIdToAnchorIdMap = attributeIdToAnchorIdMap;
        _datasetIdToAttributeSetMap = createDatasetIdToAttributeSetMap(datasetIdToMetaRecord, metaRecords);
    }
    
    
    private Map<String, Set<String>> createDatasetIdToAttributeSetMap(Map<String, String> datasetIdToMetaRecord, 
                    List<String> metaRecords) {
        Map<String, Set<String>> datasetIdToAttributeSetMap = new HashMap<String, Set<String>>();
        for (Map.Entry<String, String> entry : datasetIdToMetaRecord.entrySet()) {
            String datasetId = entry.getKey();
            String metaRecord = entry.getValue();
            Set<String> attributesSet = new HashSet<String>();
            for (String record : metaRecords) {
                String[] split = record.split("\t");
                if (split.length == 2) {
                    if (split[0].equals(metaRecord)) {
                        attributesSet.add(split[1]);
                    }
                } else {
                    LOGGER.warn(String.format("Invalid line '%s': expected 2 fields but got %d", record, split.length));
                }
            }
            if (attributesSet.size() > 0) {
                datasetIdToAttributeSetMap.put(datasetId, attributesSet);
            }
        }
        return datasetIdToAttributeSetMap;
    }


    @Override
    public List<DenaturedAttributeDatum> parse(String datasetId, String recordUuid, String attributeName, String attributeValue) {
        List<DenaturedAttributeDatum> datumsList = new ArrayList<DenaturedAttributeDatum>();
        String attributeId = null;
        String anchorId = null;
        if (_datasetIdToAttributeSetMap.containsKey(datasetId)) {
            Set<String> attributeSet = _datasetIdToAttributeSetMap.get(datasetId);
            if (_attributeNameToIdMap.containsKey(attributeName)) {
                attributeId = _attributeNameToIdMap.get(attributeName);
                if (attributeSet.contains(attributeId)) {
                    if (_attributeIdToAnchorIdMap.containsKey(attributeId)) {
                        anchorId = _attributeIdToAnchorIdMap.get(attributeId);
                    }
                } else {
                    attributeId = null; 
                }
            }
        }
        
        DenaturedAttributeDatum datum = new DenaturedAttributeDatum(datasetId, recordUuid, anchorId, attributeId, attributeValue);
        datumsList.add(datum);
        return datumsList;
    }
}
