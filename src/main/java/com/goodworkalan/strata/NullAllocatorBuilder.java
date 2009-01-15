package com.goodworkalan.strata;


final class NullAllocatorBuilder
implements AllocatorBuilder
{
    public <B, T, F extends Comparable<? super F>, A> Allocator<B, A> newAllocator(Build<B, T, F, A> build)
    {
        return new NullAllocator<B, A>();
    }
}