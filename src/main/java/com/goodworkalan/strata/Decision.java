package com.goodworkalan.strata;

interface Decision<B, A>
{
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfParent, Level<B, A> levelOfChild, InnerTier<B, A> parent);
}