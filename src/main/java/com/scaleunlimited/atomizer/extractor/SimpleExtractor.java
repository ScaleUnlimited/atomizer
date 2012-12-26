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
    private Map<String, Set<String>> _metaRecordIdToAttributeSetMap;


    public SimpleExtractor(Map<String, String>attributeNameToIdMap, Map<String, String>attributeIdToAnchorIdMap,
                    List<String>metaRecords) {
        _attributeNameToIdMap = attributeNameToIdMap;
        _attributeIdToAnchorIdMap = attributeIdToAnchorIdMap;
        _metaRecordIdToAttributeSetMap = createMetaRecordIdToAttributeSetMap(metaRecords);
    }
    
    
    private Map<String, Set<String>> createMetaRecordIdToAttributeSetMap(List<String> metaRecords) {
        Map<String, Set<String>> metaRecordIdToAttributeSetMap = new HashMap<String, Set<String>>();
        for (String record : metaRecords) {
            String[] split = record.split("\t");
            if (split.length == 2) {
                if (metaRecordIdToAttributeSetMap.containsKey(split[0])) {
                    Set<String> attributesSet = metaRecordIdToAttributeSetMap.get(split[0]);
                    attributesSet.add(split[1]);
                    metaRecordIdToAttributeSetMap.put(split[0], attributesSet);
                } else {
                    Set<String> attributesSet = new HashSet<String>();
                    attributesSet.add(split[1]);
                    metaRecordIdToAttributeSetMap.put(split[0], attributesSet);
                }
            } else {
                LOGGER.warn(String.format("Invalid line '%s': expected 2 fields but got %d", record, split.length));
            }
        }

        return metaRecordIdToAttributeSetMap;
    }


    @Override
    public List<DenaturedAttributeDatum> extract(String datasetId, String recordUuid, String metaId, String attributeName, String attributeValue) {
        List<DenaturedAttributeDatum> datumsList = new ArrayList<DenaturedAttributeDatum>();
        String attributeId = null;
        String anchorId = null;
        if (_metaRecordIdToAttributeSetMap.containsKey(metaId)) {
            Set<String> attributeSet = _metaRecordIdToAttributeSetMap.get(metaId);
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
