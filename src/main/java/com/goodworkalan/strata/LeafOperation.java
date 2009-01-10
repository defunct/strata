package com.goodworkalan.strata;

interface LeafOperation<B, A>
{
    public boolean operate(Mutation<B, A> mutation, Level<B, A> levelOfLeaf);
}