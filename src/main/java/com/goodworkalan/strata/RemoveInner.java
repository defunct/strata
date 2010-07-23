package com.goodworkalan.strata;

// TODO Document.
public final class RemoveInner<T, A>
implements Operation<T, A> {
    // TODO Document.
    private final Tier<T, A> parent;

    // TODO Document.
    private final Tier<T, A> child;

    // TODO Document.
    public RemoveInner(Tier<T, A> parent, Tier<T, A> child) {
        this.parent = parent;
        this.child = child;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation) {
        int index = parent.getIndexOfChildAddress(child.getAddress());
        parent.clear(index, 1);

        if (parent.getSize() != 0) {
            parent.setRecord(0, null);
        }

        Stage<T, A> writer = mutation.getStructure().getStage();
        writer.free(mutation.getStash(), child);
        writer.dirty(mutation.getStash(), parent);
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