package com.goodworkalan.strata;


class StorageAllocatorBuilder
implements AllocatorBuilder
{
    public <B, T, A, X> Allocator<B, A, X> newAllocator(Build<B, T, A, X> build)
    {
        return new StorageAllocator<T, B, A, X>(build.getStorage());
    }
}