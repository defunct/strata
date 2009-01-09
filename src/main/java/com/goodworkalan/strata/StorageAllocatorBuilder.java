package com.goodworkalan.strata;


class StorageAllocatorBuilder
implements AllocatorBuilder
{
    public <B, T, F extends Comparable<F>, A, X> Allocator<B, A, X> newAllocator(Build<B, T, F, A, X> build)
    {
        return new StorageAllocator<T, F, B, A, X>(build.getStorage());
    }
}