package com.goodworkalan.strata;

// TODO Document.
interface Decision<T, A>
{
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfParent, Level<T, A> levelOfChild, InnerTier<T, A> parent);
}