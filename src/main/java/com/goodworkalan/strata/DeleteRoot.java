package com.goodworkalan.strata;


/**
 * Logic for deleting the 
 */
final class DeleteRoot<B, A>
implements RootDecision<B, A>
{
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root)
    {
        if (root.getChildType() == ChildType.INNER && root.size() == 2)
        {
            Structure<B, A> structure = mutation.getStructure();
            InnerTier<B, A> first = structure.getPool().getInnerTier(mutation.getTxn(), root.get(0).getAddress());
            InnerTier<B, A> second = structure.getPool().getInnerTier(mutation.getTxn(), root.get(1).getAddress());
            // FIXME These numbers are off.
            return first.size() + second.size() == structure.getInnerSize();
        }
        return false;
    }

    public void operation(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root)
    {
        levelOfRoot.listOfOperations.add(new DeleteRoot.Merge<B, A>(root));
    }

    public final static class Merge<B, A>
    implements Operation<B, A>
    {
        private final InnerTier<B, A> root;

        public Merge(InnerTier<B, A> root)
        {
            this.root = root;
        }

        public void operate(Mutation<B, A> mutation)
        {
            if (root.size() != 0)
            {
                throw new IllegalStateException();
            }
            
            Structure<B, A> structure = mutation.getStructure();

            InnerTier<B, A> child = structure.getPool().getInnerTier(mutation.getTxn(), root.remove(0).getAddress());
            while (child.size() != 0)
            {
                root.add(child.remove(0));
            }

            root.setChildType(child.getChildType());

            TierWriter<B, A> writer = structure.getWriter();
            writer.remove(child);
            writer.dirty(mutation.getTxn(), root);
        }

        public boolean canCancel()
        {
            return true;
        }
    }
}