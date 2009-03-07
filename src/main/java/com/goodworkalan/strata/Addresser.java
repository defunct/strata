package com.goodworkalan.strata;

// TODO Document.
public interface Addresser<A>
{
    // TODO Document.
    public <B> A getAddress(InnerTier<B, A> inner);
    
    // TODO Document.
    public boolean isNull(A address);
    
    // TODO Document.
    public A getNull();
}