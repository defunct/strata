package com.goodworkalan.strata;

public interface Addresser<A>
{
    public <B> A getAddress(InnerTier<B, A> inner);
    
    public boolean isNull(A address);
    
    public A getNull();
}