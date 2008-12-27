package com.goodworkalan.strata;


public final class SplitRoot<B, A, X>
implements RootDecision<B, A, X>
{
    public boolean test(Mutation<B, A, X> mutation,
                        Level<B, A, X> levelOfRoot,
                        InnerTier<B, A> root)
    {
        return mutation.getStructure().getInnerSize() == root.size();
    }

    public void operation(Mutation<B, A, X> mutation,
                          Level<B, A, X> levelOfRoot,
                          InnerTier<B, A> root)
    {
        levelOfRoot.listOfOperations.add(new SplitRoot.Split<B, A, X>(root));
    }

    private final static class Split<B, A, X>
    implements Operation<B, A, X>
    {
        private final InnerTier<B, A> root;

        public Split(InnerTier<B, A> root)
        {
            this.root = root;
        }

        public void operate(Mutation<B, A, X> mutation)
        {
            InnerTier<B, A> left = mutation.newInnerTier(root.getChildType());
            InnerTier<B, A> right = mutation.newInnerTier(root.getChildType());
            
            int partition = root.size() / 2;
            int fullSize = root.size();
            for (int i = 0; i < partition; i++)
            {
                left.add(root.remove(0));
            }
            for (int i = partition; i < fullSize; i++)
            {
                right.add(root.remove(0));
            }
            B pivot = right.get(0).getPivot();
            right.get(0).setPivot(null);

            root.add(new Branch<B, A>(null, left.getAddress()));
            root.add(new Branch<B, A>(pivot, right.getAddress()));

            root.setChildType(ChildType.INNER);

            TierWriter<B, A, X> writer = mutation.getStructure().getWriter();
            writer.dirty(mutation.getTxn(), root);
            writer.dirty(mutation.getTxn(), left);
            writer.dirty(mutation.getTxn(), right);
        }

        public boolean canCancel()
        {
            return true;
        }
    }
}