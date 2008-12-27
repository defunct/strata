package com.goodworkalan.strata;

public interface Extractor<T, X>
{
    public void extract(X txn, T object, Record record);
}