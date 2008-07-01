/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class ArrayListStorage<T>
implements Strata.Storage<T>
{
    private static final long serialVersionUID = 20070208L;
    
    private final ArrayStore<Strata.Branch> innerStore;
    
    private final ArrayStore<T> leafStore;
    
    public ArrayListStorage(Strata.Structure structure)
    {
        this.innerStore = new ArrayStore<Strata.Branch>(structure, structure.getSchema().getInnerSize());
        this.leafStore = new ArrayStore<T>(structure, structure.getSchema().getLeafSize());
    }
    
    public Strata.Store<Strata.Branch> getBranchStore()
    {
        return innerStore;
    }
    
    public Strata.Store<T> getLeafStore()
    {
        return leafStore;
    }
    
    public Strata.Storage.Schema<T> newSchema()
    {
        return null;
    }
    
    public void commit(Object txn)
    {
    }

    public final static class ArrayListKey<T>
    implements Strata.Identifier<T>
    {
        public Object getKey(Strata.Tier<T> tier)
        {
            return tier;
        }

        public Object getNullKey()
        {
            return null;
        }

        public boolean isKeyNull(Object object)
        {
            return object == null;
        }
    }

    public final static class Schema<T>
    implements Strata.Storage.Schema<T>
    {
        public Strata.Storage<T> newStorage(Strata.Structure structure, Object txn)
        {
            return new ArrayListStorage<T>(structure);
        }
    }

    public final static class ArrayStore<T>
    implements Strata.Store<T>
    {
        private static final long serialVersionUID = 1L;

        private final int size;

        private final Strata.Structure structure;

        public ArrayStore(Strata.Structure structure, int size)
        {
            this.size = size;
            this.structure = structure;
        }

        public Strata.Identifier<T> getIdentifier()
        {
            return new ArrayListKey<T>();
        }
        
        @SuppressWarnings("unchecked")
        public Strata.Tier<T> getTier(Object txn, Object key)
        {
            return (Strata.Tier<T>) key;
        }

        public Strata.Tier<T> newTier(Object storage)
        {
            return new Strata.Tier<T>(structure, new ArrayListKey<T>(), null, size);
        }

        public void write(Object txn, Strata.Tier<T> tier)
        {
        }

        public void free(Object txn, Strata.Tier<T> tier)
        {
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
