package com.goodworkalan.strata;


public interface Transaction<T, X>
extends Query<T>
{
    public Tree<T, X> getTree();
}