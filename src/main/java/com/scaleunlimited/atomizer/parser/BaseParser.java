package com.scaleunlimited.atomizer.parser;

import java.util.List;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

public abstract class BaseParser {

    public abstract List<DenaturedAttributeDatum> parse(String datasetId, String recordUuid, String attributeName, String attributeValue);
}
