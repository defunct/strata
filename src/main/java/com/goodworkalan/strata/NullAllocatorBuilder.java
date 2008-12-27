package com.goodworkalan.strata;


final class NullAllocatorBuilder
implements AllocatorBuilder
{
    public <B, T, A, X> Allocator<B, A, X> newAllocator(Build<B, T, A, X> build)
    {
        return new NullAllocator<B, A, X>();
    }
}