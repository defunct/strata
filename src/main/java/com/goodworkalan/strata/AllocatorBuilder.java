package com.goodworkalan.strata;

// TODO Document.
public interface AllocatorBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> Allocator<B, A> newAllocator(Build<B, T, F, A> build);
}