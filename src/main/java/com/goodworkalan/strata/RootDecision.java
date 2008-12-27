package com.goodworkalan.strata;

interface RootDecision<B, A, X>
{
    public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfRoot, InnerTier<B, A> root);

    public void operation(Mutation<B, A, X> mutation, Level<B, A, X> levelOfRoot, InnerTier<B, A> root);
}