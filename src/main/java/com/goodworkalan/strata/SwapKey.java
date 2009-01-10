package com.goodworkalan.strata;


final class SwapKey<B, A>
implements Decision<B, A>
{
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

    private final static class Swap<B, A>
    implements Operation<B, A>
    {
        private final InnerTier<B, A> inner;

        public Swap(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        public void operate(Mutation<B, A> mutation)
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