/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import org.testng.annotations.Test;

import com.agtrz.strata.Strata.Record;

public class StrataTestCase
{
    @Test public void create()
    {
        Strata.Schema schema = Strata.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        Strata.Extractor<Integer, Object> extractor = new Strata.Extractor<Integer, Object>()
        {
            public void extract(Object txn, Integer object, Record record)
            {
                record.fields(object);
            }
        };
       /* Strata.Tree<Integer, Object> tree = */ schema.newTree(null, extractor);
//        Strata.Query<Integer> query = tree.query(null);
//        query.add(1);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */