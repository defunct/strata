package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public class BucketTreeBuilder
implements TreeBuilder
{
    public <T, F extends Comparable<F>, A> Query<T, F> newTransaction(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage)
    {
        Cooper<T, F, Bucket<T, F>> cooper = new BucketCooper<T, F>();
        Build<Bucket<T, F>, T, F, A> build = new Build<Bucket<T, F>, T, F, A>(schema, storage, cooper);
        return build.newTransaction(stash);
    }
}