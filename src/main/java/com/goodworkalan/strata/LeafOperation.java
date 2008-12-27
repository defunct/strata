package com.goodworkalan.strata;

interface LeafOperation<B, A, X>
{
    public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf);
}