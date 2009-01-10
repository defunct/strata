package com.goodworkalan.strata;

interface Operation<B, A>
{
    public void operate(Mutation<B, A> mutation);

    public boolean canCancel();
}