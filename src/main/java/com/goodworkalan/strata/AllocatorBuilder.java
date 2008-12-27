package com.goodworkalan.strata;


public interface AllocatorBuilder
{
    public <B, T, A, X> Allocator<B, A, X> newAllocator(Build<B, T, A, X> build);
}