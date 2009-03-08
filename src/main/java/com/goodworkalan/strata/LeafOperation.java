package com.goodworkalan.strata;

// TODO Document.
interface LeafOperation<T, A>
{
    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf);
}