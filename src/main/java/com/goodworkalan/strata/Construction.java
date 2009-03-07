package com.goodworkalan.strata;

// TODO Document.
public class Construction<T, F extends Comparable<? super F>, A>
{
    // TODO Document.
    private A address;
    
    // TODO Document.
    private Query<T, F> query;
    
    // TODO Document.
    public Construction(Query<T, F> query, A address)
    {
        this.address = address;
        this.query = query;
    }
    
    // TODO Document.
    public A getAddress()
    {
        return address;
    }
    
    // TODO Document.
    public Query<T, F> getQuery()
    {
        return query;
    }
}
