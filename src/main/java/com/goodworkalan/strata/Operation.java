package com.goodworkalan.strata;

interface Operation<B, A, X>
{
    public void operate(Mutation<B, A, X> mutation);

    public boolean canCancel();
}