package com.goodworkalan.strata;

public final class RemoveLeaf<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> parent;

    // TODO Document.
    private final LeafTier<T, A> leaf;

    // TODO Document.
    private final LeafTier<T, A> left;

    // TODO Document.
    public RemoveLeaf(InnerTier<T, A> parent, LeafTier<T, A> leaf, LeafTier<T, A> left)
    {
        this.parent = parent;
        this.leaf = leaf;
        this.left = left;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        parent.remove(parent.getIndex(leaf.getAddress()));

        left.setNext(leaf.getNext());

        Stage<T, A> writer = mutation.getStructure().getTierWriter();
        writer.free(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), parent);
        writer.dirty(mutation.getStash(), left);

        mutation.setOnlyChild(false);
    }

    // TODO Document.
    public boolean canCancel()
    {
        return true;
    }
}