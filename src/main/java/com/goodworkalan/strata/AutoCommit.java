package com.goodworkalan.strata;

interface AutoCommit<X>
{
    public void autoCommit(X txn);
}