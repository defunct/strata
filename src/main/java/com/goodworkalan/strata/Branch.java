package com.goodworkalan.strata;

// TODO Document.
public final class Branch<T, A>
{
    // TODO Document.
    private final A address;

    // TODO Document.
    private T pivot;

    // TODO Document.
    public Branch(T pivot, A address)
    {
        this.address = address;
        this.pivot = pivot;
    }

    // TODO Document.
    public A getAddress()
    {
        return address;
    }

    // TODO Document.
    public T getPivot()
    {
        return pivot;
    }

    // TODO Document.
    public void setPivot(T pivot)
    {
        this.pivot = pivot;
    }

    // TODO Document.
    public boolean isMinimal()
    {
        return pivot == null;
    }

    // TODO Document.
    public String toString()
    {
        return pivot == null ? "MINIMAL" : pivot.toString();
    }
}