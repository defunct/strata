package com.goodworkalan.strata;


/**
 * Logic for deleting the 
 */
final class ShouldDeleteRoot<B, A>
implements RootDecision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root)
    {
        if (root.getChildType() == ChildType.INNER && root.size() == 2)
        {
            Structure<B, A> structure = mutation.getStructure();
            InnerTier<B, A> first = structure.getPool().getInnerTier(mutation.getStash(), root.get(0).getAddress());
            InnerTier<B, A> second = structure.getPool().getInnerTier(mutation.getStash(), root.get(1).getAddress());
            // FIXME These numbers are off.
            return first.size() + second.size() == structure.getInnerSize();
        }
        return false;
    }

    // TODO Document.
    public void operation(Mutation<B, A> mutation, Level<B, A> levelOfRoot, InnerTier<B, A> root)
    {
        levelOfRoot.listOfOperations.add(new ShouldDeleteRoot.MergeRoot<B, A>(root));
    }

    // TODO Document.
    public final static class MergeRoot<B, A>
    implements Operation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> root;

        // TODO Document.
        public MergeRoot(InnerTier<B, A> root)
        {
            this.root = root;
        }

        // TODO Document.
        public void operate(Mutation<B, A> mutation)
        {
            if (root.size() != 0)
            {
                throw new IllegalStateException();
            }
            
            Structure<B, A> structure = mutation.getStructure();

            InnerTier<B, A> child = structure.getPool().getInnerTier(mutation.getStash(), root.remove(0).getAddress());
            while (child.size() != 0)
            {
                root.add(child.remove(0));
            }

            root.setChildType(child.getChildType());

            TierWriter<B, A> writer = structure.getWriter();
            writer.remove(child);
            writer.dirty(mutation.getStash(), root);
        }

        // TODO Document.
        public boolean canCancel()
        {
            return true;
        }
    }
}