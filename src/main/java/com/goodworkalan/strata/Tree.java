package com.goodworkalan.strata;


public interface Tree<T, X>
{
    public Query<T> query(X txn);
}