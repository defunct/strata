package com.goodworkalan.strata;

// TODO Document.
interface Operation<T, A>
{
    // TODO Document.
    public void operate(Mutation<T, A> mutation);

    // TODO Document.
    public boolean canCancel();
}