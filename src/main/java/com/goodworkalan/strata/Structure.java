package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;


interface Structure<B, A>
{
    public int getInnerSize();
    
    public int getLeafSize();
    
    public Allocator<B, A> getAllocator();
    
    public TierWriter<B, A> getWriter();
    
    public TierPool<B, A> getPool();
    
    public int compare(Stash stash, B left, B right);
}