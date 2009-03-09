package com.goodworkalan.strata;

//TODO Document.
final class SwapKey<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> inner;

    // TODO Document.
    public SwapKey(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        if (mutation.getReplacement() != null)
        {
            Branch<T, A> branch = inner.find(mutation.getComparable());
            branch.setPivot(mutation.getReplacement());
            mutation.getStructure().getStage().dirty(mutation.getStash(), inner);
        }
    }

    // TODO Document.
    public boolean isSplitOrMerge()
    {
        return false;
    }
}