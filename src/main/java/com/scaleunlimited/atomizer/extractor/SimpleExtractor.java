package com.scaleunlimited.atomizer.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

@SuppressWarnings("serial")
public class SimpleExtractor extends BaseExtractor {

    private static final Logger LOGGER = Logger.getLogger(SimpleExtractor.class);

    private Map<String, String> _attributeNameToIdMap;
    private Map<String, String> _attributeIdToAnchorIdMap;
    private Map<String, Set<String>> _attributeRecordIdToAttributeSetMap;


    public SimpleExtractor(Map<String, String>attributeNameToIdMap, Map<String, String>attributeIdToAnchorIdMap,
                    List<String>attributeRecords) {
        _attributeNameToIdMap = attributeNameToIdMap;
        _attributeIdToAnchorIdMap = attributeIdToAnchorIdMap;
        _attributeRecordIdToAttributeSetMap = createAttributeRecordIdToAttributeSetMap(attributeRecords);
    }
    
    
    private Map<String, Set<String>> createAttributeRecordIdToAttributeSetMap(List<String> attributeRecords) {
        Map<String, Set<String>> attributeRecordIdToAttributeSetMap = new HashMap<String, Set<String>>();
        for (String record : attributeRecords) {
            String[] split = record.split("\t");
            if (split.length == 2) {
                if (attributeRecordIdToAttributeSetMap.containsKey(split[0])) {
                    Set<String> attributesSet = attributeRecordIdToAttributeSetMap.get(split[0]);
                    attributesSet.add(split[1]);
                    attributeRecordIdToAttributeSetMap.put(split[0], attributesSet);
                } else {
                    Set<String> attributesSet = new HashSet<String>();
                    attributesSet.add(split[1]);
                    attributeRecordIdToAttributeSetMap.put(split[0], attributesSet);
                }
            } else {
                LOGGER.warn(String.format("Invalid line '%s': expected 2 fields but got %d", record, split.length));
            }
        }

        return attributeRecordIdToAttributeSetMap;
    }


    @Override
    public List<DenaturedAttributeDatum> extract(String datasetId, String recordUuid, String attributeRecordId, String attributeName, String attributeValue) {
        List<DenaturedAttributeDatum> datumsList = new ArrayList<DenaturedAttributeDatum>();
        String attributeId = null;
        String anchorId = null;
        if (_attributeRecordIdToAttributeSetMap.containsKey(attributeRecordId)) {
            Set<String> attributeSet = _attributeRecordIdToAttributeSetMap.get(attributeRecordId);
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
