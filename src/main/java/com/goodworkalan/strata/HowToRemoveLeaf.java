package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.List;

// TODO Document.
final class HowToRemoveLeaf<T, A>
implements Decision<T, A>
{
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> parentLevel, Level<T, A> childLevel, InnerTier<T, A> parent)
    {
        Structure<T, A> structure = mutation.getStructure();
        Pool<T, A> pool = structure.getPool();
        
        childLevel.locker = new WriteLockExtractor();
        Branch<T, A> branch = parent.find(mutation.getComparable());
        int index = parent.getIndex(branch.getAddress());
        LeafTier<T, A> previous = null;
        LeafTier<T, A> leaf = null;
        List<LeafTier<T, A>> listToMerge = new ArrayList<LeafTier<T, A>>();
        if (index != 0)
        {
            previous = pool.getLeafTier(mutation.getStash(), parent.get(index - 1).getAddress());
            childLevel.lockAndAdd(previous);
            leaf = pool.getLeafTier(mutation.getStash(), branch.getAddress());
            childLevel.lockAndAdd(leaf);
            int capacity = previous.size() + leaf.size();
            if (capacity <= structure.getLeafSize() + 1)
            {
                listToMerge.add(previous);
                listToMerge.add(leaf);
            }
            else
            {
                childLevel.unlockAndRemove(previous);
            }
        }

        if (leaf == null)
        {
            leaf = pool.getLeafTier(mutation.getStash(), branch.getAddress());
            childLevel.lockAndAdd(leaf);
        }

        // TODO Do not need the parent size test, just need deleting.
        if (leaf.size() == 1 && parent.size() == 1 && mutation.isDeleting())
        {
            LeafTier<T, A> left = mutation.getLeftLeaf();
            if (left == null)
            {
                mutation.setOnlyChild(true);
                mutation.leafOperation = new FailedLeafOperation<T, A>();
                return false;
            }

            parentLevel.operations.add(new RemoveLeaf<T, A>(parent, leaf, left));
            mutation.leafOperation = new RemoveObject<T, A>(leaf);
            return true;
        }
        else if (listToMerge.isEmpty() && index != parent.size() - 1)
        {
            LeafTier<T, A> next = pool.getLeafTier(mutation.getStash(), parent.get(index + 1).getAddress());
            childLevel.lockAndAdd(next);
            int capacity = next.size() + leaf.size();
            if (capacity <= structure.getLeafSize() + 1)
            {
                listToMerge.add(leaf);
                listToMerge.add(next);
            }
            else
            {
                childLevel.unlockAndRemove(next);
            }
        }

        if (listToMerge.isEmpty())
        {
            mutation.leafOperation = new RemoveObject<T, A>(leaf);
        }
        else
        {
            // TODO Test that this activates.
            if (mutation.isDeleting())
            {
                mutation.rewind(2);
                mutation.setDeleting(false);
            }
            LeafTier<T, A> left = listToMerge.get(0);
            LeafTier<T, A> right = listToMerge.get(1);
            parentLevel.operations.add(new MergeLeaf<T, A>(parent, left, right));
            mutation.leafOperation = new RemoveObject<T, A>(leaf);
        }
        return !listToMerge.isEmpty();
    }
}