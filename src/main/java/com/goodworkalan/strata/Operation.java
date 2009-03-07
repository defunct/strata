package com.goodworkalan.strata;

// TODO Document.
interface Operation<B, A>
{
    // TODO Document.
    public void operate(Mutation<B, A> mutation);

    // TODO Document.
    public boolean canCancel();
}