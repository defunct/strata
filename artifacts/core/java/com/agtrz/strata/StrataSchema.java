package com.agtrz.strata;

import java.util.HashMap;
import java.util.Map;

public class StrataSchema
{
    private final Map mapOfStratifiers = new HashMap();
    
    public void addStratifier(Class type, Stratifier stratifier)
    {
        mapOfStratifiers.put(type, stratifier);
    }
    
    public Stratifier getStratifier(Class type)
    {
        return (Stratifier) mapOfStratifiers.get(type);
    }
}

/* vim: set et sw=4 ts=4 ai tw=68: */