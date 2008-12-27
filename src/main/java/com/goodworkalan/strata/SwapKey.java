package com.goodworkalan.strata;


final class SwapKey<B, A, X>
implements Decision<B, A, X>
{
    public boolean test(Mutation<B, A, X> mutation,
                        Level<B, A, X> levelOfParent,
                        Level<B, A, X> levelOfChild,
                        InnerTier<B, A> parent)
    {
        Branch<B, A> branch = parent.find(mutation.getComparable());
        if (branch.getPivot() != null && mutation.getComparable().compareTo(branch.getPivot()) == 0)
        {
            levelOfParent.listOfOperations.add(new SwapKey.Swap<B, A, X>(parent));
            return true;
        }
        return false;
    }

    private final static class Swap<B, A, X>
    implements Operation<B, A, X>
    {
        private final InnerTier<B, A> inner;

        public Swap(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        public void operate(Mutation<B, A, X> mutation)
        {
            if (mutation.getReplacement() != null)
            {
                Branch<B, A> branch = inner.find(mutation.getComparable());
                branch.setPivot(mutation.getReplacement());
                mutation.getStructure().getWriter().dirty(mutation.getTxn(), inner);
            }
        }

        public boolean canCancel()
        {
            return false;
        }
    }
}