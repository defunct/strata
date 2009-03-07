package com.goodworkalan.strata;

// TODO Document.
interface LeafOperation<B, A>
{
    // TODO Document.
    public boolean operate(Mutation<B, A> mutation, Level<B, A> levelOfLeaf);
}