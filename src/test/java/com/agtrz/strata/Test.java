/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.Random;

import com.agtrz.strata.Strata.Query;

public class Test
{
    public interface TransactionServer
    {
        public Object newTransaction();
    }

    public final static class Environment
    {
        public final int iterations;

        public final Strata strata;

        public final TransactionServer transactionServer;

        public Environment(Strata strata, TransactionServer transactionServer)
        {
            this.iterations = 1000;
            this.strata = strata;
            this.transactionServer = transactionServer;
        }

        public Query newQuery()
        {
            Object txn = transactionServer.newTransaction();
            return strata.query(txn);
        }
    }

    public final static class Stressor
    {
        private final Random random = new Random();

        private final Environment environment;

        private Strata.Query query;

        public Stressor(Environment environment)
        {
            this.environment = environment;
        }

        public void run()
        {
            for (int i = 0; i < environment.iterations; i++)
            {
                int operationType = random.nextInt(100);
                if (operationType < 60)
                {
                }
            }
        }

        public void flush()
        {
            query.flush();
        }

        public void newQuery()
        {
            query.flush();
            query = environment.newQuery();
        }
    }

    public final static class StringArrayExtractor
    implements Strata.FieldExtractor
    {
        private final int fields;

        public StringArrayExtractor(int fields)
        {
            this.fields = fields;
        }

        public Comparable<?>[] getFields(Object txn, Object object)
        {
            Comparable<?>[] incoming = (Comparable[]) object;
            Comparable<?>[] outgoing = new Comparable[fields];
            for (int i = 0; i < fields; i++)
            {
                outgoing[i] = incoming[i];
            }
            return outgoing;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */