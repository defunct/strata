package com.goodworkalan.strata;


class LookupTreeBuilder
implements TreeBuilder
{
    public <T, A, X> Transaction<T, X> newTransaction(X txn, Schema<T, X> schema, Storage<T, A, X> storage)
    {
        Cooper<T, T, X> cooper = new LookupCooper<T, X>();
        Build<T, T, A, X> build = new Build<T, T, A, X>(schema, storage, cooper);
        return build.newTransaction(txn);
    }
}