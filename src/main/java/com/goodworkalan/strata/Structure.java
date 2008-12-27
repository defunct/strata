package com.goodworkalan.strata;


interface Structure<B, A, X>
{
    public int getInnerSize();
    
    public int getLeafSize();
    
    public Allocator<B, A, X> getAllocator();
    
    public TierWriter<B, A, X> getWriter();
    
    public TierPool<B, A, X> getPool();
    
    public int compare(X txn, B left, B right);
}