/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.Serializable;
import java.util.Collection;

/**
 * Interface for the creation and storage of tiers.
 * <p>
 * Now, more than ever, we need to make a Tier class so that this
 * interface reads and writes a series of records, using a reader and
 * writer, rather than having separate classes for inner tier and leaf
 * tier.
 */
public interface Store<A, H, T, X>
extends Serializable
{
    public A allocate(X txn);

    public H load(X txn, A address, Collection<T> collection);

    public void write(X txn, A address, H header, Collection<T> collection);

    public void free(X txn, A address);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */