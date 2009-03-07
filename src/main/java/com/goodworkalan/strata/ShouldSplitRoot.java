package com.goodworkalan.strata;

// TODO Document.
public final class ShouldSplitRoot<B, A>
implements RootDecision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation,
                        Level<B, A> levelOfRoot,
                        InnerTier<B, A> root)
    {
        return mutation.getStructure().getInnerSize() == root.size();
    }

    // TODO Document.
    public void operation(Mutation<B, A> mutation,
                          Level<B, A> levelOfRoot,
                          InnerTier<B, A> root)
    {
        levelOfRoot.listOfOperations.add(new ShouldSplitRoot.SplitRoot<B, A>(root));
    }

    // TODO Document.
    private final static class SplitRoot<B, A>
    implements Operation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> root;

        // TODO Document.
        public SplitRoot(InnerTier<B, A> root)
        {
            this.root = root;
        }

        // TODO Document.
        public void operate(Mutation<B, A> mutation)
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

            TierWriter<B, A> writer = mutation.getStructure().getWriter();
            writer.dirty(mutation.getStash(), root);
            writer.dirty(mutation.getStash(), left);
            writer.dirty(mutation.getStash(), right);
        }

        // TODO Document.
        public boolean canCancel()
        {
            return true;
        }
    }
}