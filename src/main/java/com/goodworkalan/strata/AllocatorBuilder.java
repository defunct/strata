package com.goodworkalan.strata;


public interface AllocatorBuilder
{
    public <B, T, F extends Comparable<F>, A, X> Allocator<B, A, X> newAllocator(Build<B, T, F, A, X> build);
}