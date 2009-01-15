package com.goodworkalan.strata;


public interface AllocatorBuilder
{
    public <B, T, F extends Comparable<? super F>, A> Allocator<B, A> newAllocator(Build<B, T, F, A> build);
}