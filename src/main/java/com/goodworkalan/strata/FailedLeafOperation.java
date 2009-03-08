package com.goodworkalan.strata;

public final class FailedLeafOperation<T, A>
implements LeafOperation<T, A>
{
    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        return false;
    }
}