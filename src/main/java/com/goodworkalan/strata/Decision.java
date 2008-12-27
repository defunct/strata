package com.goodworkalan.strata;

interface Decision<B, A, X>
{
    public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild, InnerTier<B, A> parent);
}