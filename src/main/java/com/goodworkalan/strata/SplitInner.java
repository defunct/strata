package com.goodworkalan.strata;

// TODO Document.
final class SplitInner<B, A>
implements Decision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfParent, Level<B, A> levelOfChild, InnerTier<B, A> parent)
    {
        Structure<B, A> structure = mutation.getStructure();
        Branch<B, A> branch = parent.find(mutation.getComparable());
        InnerTier<B, A> child = structure.getPool().getInnerTier(mutation.getTxn(), branch.getAddress());
        levelOfChild.lockAndAdd(child);
        if (child.size() == structure.getInnerSize())
        {
            levelOfParent.listOfOperations.add(new SplitInner.Split<B, A>(parent, child));
            return true;
        }
        return false;
    }

    // TODO Document.
    public final static class Split<B, A>
    implements Operation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> parent;

        // TODO Document.
        private final InnerTier<B, A> child;

        // TODO Document.
        public Split(InnerTier<B, A> parent, InnerTier<B, A> child)
        {
            this.parent = parent;
            this.child = child;
        }

        // TODO Document.
        public void operate(Mutation<B, A> mutation)
        {
            InnerTier<B, A> right = mutation.newInnerTier(child.getChildType());

            int partition = child.size() / 2;

            while (partition < child.size())
            {
                right.add(child.remove(partition));
            }

            B pivot = right.get(0).getPivot();
            right.get(0).setPivot(null);

            int index = parent.getIndex(child.getAddress());
            parent.add(index + 1, new Branch<B, A>(pivot, right.getAddress()));

            TierWriter<B, A> writer = mutation.getStructure().getWriter();
            writer.dirty(mutation.getTxn(), parent);
            writer.dirty(mutation.getTxn(), child);
            writer.dirty(mutation.getTxn(), right);
        }

        // TODO Document.
        public boolean canCancel()
        {
            return true;
        }
    }
}