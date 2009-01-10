package com.goodworkalan.strata;

interface RootDecision<B, A>
{
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root);

    public void operation(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root);
}