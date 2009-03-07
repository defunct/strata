package com.goodworkalan.strata;

// TODO Document.
class StorageAllocatorBuilder
implements AllocatorBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> Allocator<B, A> newAllocator(Build<B, T, F, A> build)
    {
        return new StorageAllocator<T, F, B, A>(build.getStorage());
    }
}