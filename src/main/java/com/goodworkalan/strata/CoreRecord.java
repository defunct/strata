package com.goodworkalan.strata;

final class CoreRecord
implements Record
{
    private Comparable<?>[] fields;

    public void fields(Comparable<?>... fields)
    {
        this.fields = fields;
    }
    
    public Comparable<?>[] getFields()
    {
        return fields;
    }
}