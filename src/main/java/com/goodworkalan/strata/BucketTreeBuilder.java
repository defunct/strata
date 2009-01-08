package com.goodworkalan.strata;

public class BucketTreeBuilder
implements TreeBuilder
{
    public <T, A, X> Transaction<T, X> newTransaction(X txn, Schema<T, X> schema, Storage<T, A, X> storage)
    {
        Cooper<T, Bucket<T>, X> cooper = new BucketCooper<T, X>();
        Build<Bucket<T>, T, A, X> build = new Build<Bucket<T>, T, A, X>(schema, storage, cooper);
        return build.newTransaction(txn);
    }
}