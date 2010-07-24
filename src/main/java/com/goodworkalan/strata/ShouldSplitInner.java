package com.goodworkalan.strata;

// TODO Document.
final class ShouldSplitInner<T, A>
implements Decision<T, A> {
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfParent, Level<T, A> levelOfChild, Tier<T, A> parent) {
        Structure<T, A> structure = mutation.getStructure();
        int branch = parent.find(mutation.getComparable());
        Tier<T, A> child = structure.getStorage().load(mutation.getStash(), parent.getChildAddress(branch));
        levelOfChild.lockAndAdd(child);
        if (child.getSize() == structure.getInnerSize()) {
            levelOfParent.operations.add(new SplitInner<T, A>(parent, child));
            return true;
        }
        return false;
    }
}