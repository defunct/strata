package com.goodworkalan.strata;

public final class MergeLeaf<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> parent;

    // TODO Document.
    private final LeafTier<T, A> left;

    // TODO Document.
    private final LeafTier<T, A> right;

    // TODO Document.
    public MergeLeaf(InnerTier<T, A> parent, LeafTier<T, A> left, LeafTier<T, A> right)
    {
        this.parent = parent;
        this.left = left;
        this.right = right;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        parent.remove(parent.getIndex(right.getAddress()));

        while (right.size() != 0)
        {
            left.add(right.remove(0));
        }
        // FIXME Get last leaf. 
        left.setNext(right.getNext());

        TierWriter<T, A> writer = mutation.getStructure().getTierWriter();
        writer.free(mutation.getStash(), right);
        writer.dirty(mutation.getStash(), parent);
        writer.dirty(mutation.getStash(), left);
    }

    // TODO Document.
    public boolean canCancel()
    {
        return true;
    }
}