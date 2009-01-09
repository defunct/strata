package com.goodworkalan.strata;

public class BucketTreeBuilder
implements TreeBuilder
{
    public <T, F extends Comparable<F>, A, X> Transaction<T, F, X> newTransaction(X txn, Schema<T, F, X> schema, Storage<T, F, A, X> storage)
    {
        Cooper<T, F, Bucket<T, F>, X> cooper = new BucketCooper<T, F, X>();
        Build<Bucket<T, F>, T, F, A, X> build = new Build<Bucket<T, F>, T, F, A, X>(schema, storage, cooper);
        return build.newTransaction(txn);
    }
}