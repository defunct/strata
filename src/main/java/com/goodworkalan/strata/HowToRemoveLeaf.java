package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.List;

// TODO Document.
final class HowToRemoveLeaf<T, A>
implements Decision<T, A> {
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> parentLevel, Level<T, A> childLevel, Tier<T, A> parent) {
        Structure<T, A> structure = mutation.getStructure();
        Pool<T, A> pool = structure.getPool();
        
        childLevel.locker = new WriteLockExtractor();
        int branch = parent.find(mutation.getComparable());
        int index = parent.getIndexOfChildAddress(parent.getChildAddress(branch));
        Tier<T, A> previous = null;
        Tier<T, A> leaf = null;
        List<Tier<T, A>> listToMerge = new ArrayList<Tier<T, A>>();
        if (index != 0) {
            previous = pool.get(mutation.getStash(), parent.getChildAddress(index - 1));
            childLevel.lockAndAdd(previous);
            leaf = pool.get(mutation.getStash(), parent.getChildAddress(branch));
            childLevel.lockAndAdd(leaf);
            int capacity = previous.getSize() + leaf.getSize();
            if (capacity <= structure.getLeafSize() + 1) {
                listToMerge.add(previous);
                listToMerge.add(leaf);
            } else {
                childLevel.unlockAndRemove(previous);
            }
        }

        if (leaf == null) {
            leaf = pool.get(mutation.getStash(), parent.getChildAddress(branch));
            childLevel.lockAndAdd(leaf);
        }

        // TODO Do not need the parent size test, just need deleting.
        if (leaf.getSize() == 1 && parent.getSize() == 1 && mutation.isDeleting()) {
            Tier<T, A> left = mutation.getLeftLeaf();
            if (left == null) {
                mutation.setOnlyChild(true);
                mutation.leafOperation = new FailedLeafOperation<T, A>();
                return false;
            }

            parentLevel.operations.add(new RemoveLeaf<T, A>(parent, leaf, left));
            mutation.leafOperation = new RemoveObject<T, A>(leaf);
            return true;
        } else if (listToMerge.isEmpty() && index != parent.getSize() - 1) {
            Tier<T, A> next = pool.get(mutation.getStash(), parent.getChildAddress(index + 1));
            childLevel.lockAndAdd(next);
            int capacity = next.getSize() + leaf.getSize();
            if (capacity <= structure.getLeafSize() + 1) {
                listToMerge.add(leaf);
                listToMerge.add(next);
            } else {
                childLevel.unlockAndRemove(next);
            }
        }

        if (listToMerge.isEmpty()) {
            mutation.leafOperation = new RemoveObject<T, A>(leaf);
        } else {
            // TODO Test that this activates.
            if (mutation.isDeleting()) {
                mutation.rewind(2);
                mutation.setDeleting(false);
            }
            Tier<T, A> left = listToMerge.get(0);
            Tier<T, A> right = listToMerge.get(1);
            parentLevel.operations.add(new MergeLeaf<T, A>(parent, left, right));
            mutation.leafOperation = new RemoveObject<T, A>(leaf);
        }
        return !listToMerge.isEmpty();
    }
}