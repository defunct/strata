/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public final class Branch<T, K>
{
    private final K key;

    private T pivot;

    public Branch(K key, T pivot)
    {
        this.key = key;
        this.pivot = pivot;
    }

    public K getRightKey()
    {
        return key;
    }

    public T getPivot()
    {
        return pivot;
    }

    public void setPivot(T pivot)
    {
        this.pivot = pivot;
    }

    public boolean isMinimal()
    {
        return pivot == null;
    }

    public String toString()
    {
        return pivot == null ? "MINIMAL" : pivot.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */