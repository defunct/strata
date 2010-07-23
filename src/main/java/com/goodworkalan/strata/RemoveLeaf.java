package com.goodworkalan.strata;

// TODO Document.
public final class RemoveLeaf<T, A>
implements Operation<T, A> {
    // TODO Document.
    private final Tier<T, A> parent;

    // TODO Document.
    private final Tier<T, A> leaf;

    // TODO Document.
    private final Tier<T, A> left;

    // TODO Document.
    public RemoveLeaf(Tier<T, A> parent, Tier<T, A> leaf, Tier<T, A> left) {
        this.parent = parent;
        this.leaf = leaf;
        this.left = left;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation) {
        parent.clear(parent.getIndexOfChildAddress(leaf.getAddress()), 1);

        left.setNext(leaf.getNext());

        Stage<T, A> writer = mutation.getStructure().getStage();
        writer.free(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), parent);
        writer.dirty(mutation.getStash(), left);

        mutation.setOnlyChild(false);
    }

    /**
     * Return true indicating that this is a merge operation.
     * 
     * @return True indicating that this is a merge operation.
     */
    public boolean isSplitOrMerge() {
        return true;
    }
}