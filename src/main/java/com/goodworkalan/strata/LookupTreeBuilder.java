package com.goodworkalan.strata;


class LookupTreeBuilder
implements TreeBuilder
{
    public <T, F extends Comparable<F>, A, X> Transaction<T, F, X> newTransaction(X txn, Schema<T, F, X> schema, Storage<T, F, A, X> storage)
    {
        Cooper<T, F, T, X> cooper = new LookupCooper<T, F, X>();
        Build<T, T, F, A, X> build = new Build<T, T, F, A, X>(schema, storage, cooper);
        return build.newTransaction(txn);
    }
}