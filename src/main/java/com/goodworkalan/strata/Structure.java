package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
interface Structure<B, A>
{
    // TODO Document.
    public int getInnerSize();
    
    // TODO Document.
    public int getLeafSize();
    
    // TODO Document.
    public Allocator<B, A> getAllocator();
    
    // TODO Document.
    public TierWriter<B, A> getWriter();
    
    // TODO Document.
    public TierPool<B, A> getPool();
    
    // TODO Document.
    public int compare(Stash stash, B left, B right);
}