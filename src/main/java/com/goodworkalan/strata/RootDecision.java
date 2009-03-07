package com.goodworkalan.strata;

// TODO Document.
interface RootDecision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root);

    // TODO Document.
    public void operation(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root);
}