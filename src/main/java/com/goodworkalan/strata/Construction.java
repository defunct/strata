package com.goodworkalan.strata;

public class Construction<T, F extends Comparable<F>, A>
{
    private A address;
    
    private Query<T, F> query;
    
    public Construction(Query<T, F> query, A address)
    {
        this.address = address;
        this.query = query;
    }
    
    public A getAddress()
    {
        return address;
    }
    
    public Query<T, F> getQuery()
    {
        return query;
    }
}
