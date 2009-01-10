package com.goodworkalan.strata;


class StorageAllocatorBuilder
implements AllocatorBuilder
{
    public <B, T, F extends Comparable<F>, A> Allocator<B, A> newAllocator(Build<B, T, F, A> build)
    {
        return new StorageAllocator<T, F, B, A>(build.getStorage());
    }
}