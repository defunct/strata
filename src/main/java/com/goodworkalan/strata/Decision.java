package com.goodworkalan.strata;

// TODO Document.
interface Decision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfParent, Level<B, A> levelOfChild, InnerTier<B, A> parent);
}