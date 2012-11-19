package com.scaleunlimited.atomizer.parser;

import java.util.List;

import com.scaleunlimited.atomizer.datum.AnchorDatum;

public abstract class BaseParser {

    public abstract List<AnchorDatum> parse(String attributeName, String attributeValue);
}
