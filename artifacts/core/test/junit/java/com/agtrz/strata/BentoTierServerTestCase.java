/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import com.agtrz.bento.Bento;
import com.agtrz.strata.Strata.Criteria;
import com.agtrz.swag.io.SizeOf;
import com.agtrz.swag.util.Converter;
import com.agtrz.swag.util.Pair;

public class BentoTierServerTestCase
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
        public Criteria newCriteria(Object object)
        {
            return new PairCriteria((Pair) object);
        }
    }

    private final static class PairCriteria
    implements Strata.Criteria
    {
        private final Pair pair;

        public PairCriteria(Pair pair)
        {
            this.pair = pair;
        }

        public int partialMatch(Object object)
        {
            return ((Comparable) pair.getValue()).compareTo((Integer) object);
        }

        public Object getObject()
        {
            return pair;
        }

        public boolean exactMatch(Object object)
        {

            return ((Pair) object).getValue().equals(pair.getValue());
        }

        public int hashCode()
        {
            return super.hashCode();
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

    public void testConstruction()
    {
        File file = newFile();
        Bento.Creator creator = new Bento.Creator();
        Bento bento = creator.create(file);
        Bento.Mutator mutator = bento.mutate();

        Converter getAddress = new Converter()
        {
            public Object convert(Object object)
            {
                return ((Pair) object).getKey();
            }
        };
        Strata.ObjectLoader getInt = new Strata.ObjectLoader()
        {
            public Object load(Object txn, Object key)
            {
                Bento.Mutator mutator = (Bento.Mutator) txn;
                Bento.Address address = (Bento.Address) key;
                return new Integer(mutator.load(address).toByteBuffer().getInt());
            }
        };
        BentoStorage.Creator newBentoStorage = new BentoStorage.Creator();
        newBentoStorage.setBlockLoader(getInt);
        newBentoStorage.setAddressConverter(getAddress);

        Strata.Creator newStrata = new Strata.Creator();

        newStrata.setCriteriaServer(new PairCriteriaServer());

        newStrata.setStorage(newBentoStorage.create());

        Strata strata = newStrata.create(mutator);

        Bento.Block block = mutator.allocate(SizeOf.INTEGER);
        block.toByteBuffer().putInt(1);
        block.write();

        Strata.Query query = strata.query(mutator);

        query.insert(new PairCriteria(new Pair(block.getAddress(), new Integer(1))));

        Iterator found = query.find(new PairQuery(new Integer(1))).iterator();
        while (found.hasNext())
        {
            System.out.println(((Pair) found.next()).getValue());
        }

        mutator.getJournal().commit();
        bento.close();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */