package com.goodworkalan.strata;

public final class Branch<T, A>
{
    private final A address;

    private T pivot;

    public Branch(T pivot, A address)
    {
        this.address = address;
        this.pivot = pivot;
    }

    public A getAddress()
    {
        return address;
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