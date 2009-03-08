package com.goodworkalan.strata;

// TODO Document.
interface RootDecision<T, A>
{
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfRoot, InnerTier<T, A> root);

    // TODO Document.
    public void operation(Mutation<T, A> mutation, Level<T, A> levelOfRoot, InnerTier<T, A> root);
}