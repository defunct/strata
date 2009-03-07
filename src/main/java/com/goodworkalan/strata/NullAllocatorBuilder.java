package com.goodworkalan.strata;

// TODO Document.
final class NullAllocatorBuilder
implements AllocatorBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> Allocator<B, A> newAllocator(Build<B, T, F, A> build)
    {
        return new NullAllocator<B, A>();
    }
}