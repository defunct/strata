/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import com.agtrz.bento.Bento;
import com.agtrz.strata.Strata.Criteria;
import com.agtrz.strata.bento.BentoStorage;
import com.agtrz.swag.io.SizeOf;
import com.agtrz.swag.util.Pair;

public class BentoStorageTestCase
extends TestCase
{
    private File newFile()
    {
        try
        {
            File file = File.createTempFile("momento", ".mto");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final static class PairCriteriaServer
    implements Strata.CriteriaServer
    {
        public Criteria newCriteria(Object txn, Object object)
        {
            return newCriteria(new PairComparison(), txn, object);
        }

        public Criteria newCriteria(Strata.Comparison comparison, Object txn, Object object)
        {
            return new Strata.ComplexCriteria(new Strata.BasicResolver(), comparison, txn, object);
        }
    }

    private final static class PairComparison
    implements Strata.Comparison
    {
        public int partialMatch(Object criteria, Object stored)
        {
            Comparable left = (Comparable) ((Pair) criteria).getValue();
            Comparable right = (Comparable) ((Pair) stored).getValue();
            return left.compareTo(right);
        }

        public boolean exactMatch(Object criteria, Object stored)
        {
            Comparable left = (Comparable) ((Pair) criteria).getValue();
            Comparable right = (Comparable) ((Pair) stored).getValue();
            return left.equals(right);
        }
    }

    public final static class PairQuery
    implements Strata.Criteria
    {
        private final Comparable criteria;

        public PairQuery(Comparable criteria)
        {
            this.criteria = criteria;
        }

        public int partialMatch(Object object)
        {
            return criteria.compareTo(((Pair) object).getValue());
        }

        public boolean exactMatch(Object object)
        {
            return criteria.equals(((Pair) object).getValue());
        }

        public Object getObject()
        {
            return criteria;
        }
    }

    public void testUsingPair()
    {
        File file = newFile();
        Bento.Creator creator = new Bento.Creator();
        Bento bento = creator.create(file);
        Bento.Mutator mutator = bento.mutate();

        BentoStorage.Creator newBentoStorage = new BentoStorage.Creator();

        Strata.Creator newStrata = new Strata.Creator();

        newStrata.setCriteriaServer(new PairCriteriaServer());

        newStrata.setStorage(newBentoStorage.create());

        Strata strata = newStrata.create(mutator);

        Bento.Block block = mutator.allocate(SizeOf.INTEGER);
        block.toByteBuffer().putInt(1);
        block.write();

        Strata.Query query = strata.query(mutator);

        query.insert(new Pair(block.getAddress(), new Integer(1)));

        Iterator found = query.find(new PairQuery(new Integer(1)));
        while (found.hasNext())
        {
            System.out.println(((Pair) found.next()).getValue());
        }

        query.write();

        mutator.getJournal().commit();
        bento.close();

        Bento.Opener opener = new Bento.Opener();
        bento = opener.open(file);

        bento.close();
    }

    public void testUsingLookup()
    {

    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */