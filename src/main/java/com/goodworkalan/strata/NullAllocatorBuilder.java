package com.goodworkalan.strata;


final class NullAllocatorBuilder
implements AllocatorBuilder
{
    public <B, T, F extends Comparable<F>, A, X> Allocator<B, A, X> newAllocator(Build<B, T, F, A, X> build)
    {
        return new NullAllocator<B, A, X>();
    }
}