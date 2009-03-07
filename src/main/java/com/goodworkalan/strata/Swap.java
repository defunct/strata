package com.goodworkalan.strata;

final class Swap<B, A>
implements Operation<B, A>
{
    // TODO Document.
    private final InnerTier<B, A> inner;

    // TODO Document.
    public Swap(InnerTier<B, A> inner)
    {
        this.inner = inner;
    }

    // TODO Document.
    public void operate(Mutation<B, A> mutation)
    {
        if (mutation.getReplacement() != null)
        {
            Branch<B, A> branch = inner.find(mutation.getComparable());
            branch.setPivot(mutation.getReplacement());
            mutation.getStructure().getWriter().dirty(mutation.getTxn(), inner);
        }
    }

    // TODO Document.
    public boolean canCancel()
    {
        return false;
    }
}