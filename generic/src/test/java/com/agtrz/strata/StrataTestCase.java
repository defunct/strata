/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import org.junit.Test;

import com.agtrz.strata.Strata.Record;

public class StrataTestCase
{
    @Test public void create()
    {
        Strata.Schema<Integer, Object> schema = Strata.newInMemoryTree(Integer.class);
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        schema.setCacheFields(true);
        schema.setExtractor(new Strata.Extractor<Integer, Object>()
        {
            public void extract(Object txn, Integer object, Record record)
            {
                record.fields(object);
            }
        });
        Strata.Tree<Integer, Object> tree = schema.newTree();
        Strata.Query<Integer> query = tree.query(null);
        query.add(1);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */