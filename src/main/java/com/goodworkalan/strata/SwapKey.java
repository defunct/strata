package com.goodworkalan.strata;

// TODO Document.
final class SwapKey<B, A>
implements Decision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation,
                        Level<B, A> levelOfParent,
                        Level<B, A> levelOfChild,
                        InnerTier<B, A> parent)
    {
        Branch<B, A> branch = parent.find(mutation.getComparable());
        if (branch.getPivot() != null && mutation.getComparable().compareTo(branch.getPivot()) == 0)
        {
            levelOfParent.listOfOperations.add(new SwapKey.Swap<B, A>(parent));
            return true;
        }
        return false;
    }

    // TODO Document.
    private final static class Swap<B, A>
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
}