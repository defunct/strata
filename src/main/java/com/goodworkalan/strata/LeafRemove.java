package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class LeafRemove<B, A, X>
implements Decision<B, A, X>
{
    public boolean test(Mutation<B, A, X> mutation,
                        Level<B, A, X> levelOfParent,
                        Level<B, A, X> levelOfChild,
                        InnerTier<B, A> parent)
    {
        Structure<B, A, X> structure = mutation.getStructure();
        TierPool<B, A, X> pool = structure.getPool();
        
        levelOfChild.getSync = new WriteLockExtractor();
        Branch<B, A> branch = parent.find(mutation.getComparable());
        int index = parent.getIndex(branch.getAddress());
        LeafTier<B, A> previous = null;
        LeafTier<B, A> leaf = null;
        List<LeafTier<B, A>> listToMerge = new ArrayList<LeafTier<B, A>>();
        if (index != 0)
        {
            previous = pool.getLeafTier(mutation.getTxn(), parent.get(index - 1).getAddress());
            levelOfChild.lockAndAdd(previous);
            leaf = pool.getLeafTier(mutation.getTxn(), branch.getAddress());
            levelOfChild.lockAndAdd(leaf);
            int capacity = previous.size() + leaf.size();
            if (capacity <= structure.getLeafSize() + 1)
            {
                listToMerge.add(previous);
                listToMerge.add(leaf);
            }
            else
            {
                levelOfChild.unlockAndRemove(previous);
            }
        }

        if (leaf == null)
        {
            leaf = pool.getLeafTier(mutation.getTxn(), branch.getAddress());
            levelOfChild.lockAndAdd(leaf);
        }

        // TODO Do not need the parent size test, just need deleting.
        if (leaf.size() == 1 && parent.size() == 1 && mutation.isDeleting())
        {
            LeafTier<B, A> left = mutation.getLeftLeaf();
            if (left == null)
            {
                mutation.setOnlyChild(true);
                mutation.leafOperation = new LeafRemove.Fail<B, A, X>();
                return false;
            }

            levelOfParent.listOfOperations.add(new LeafRemove.RemoveLeaf<B, A, X>(parent, leaf, left));
            mutation.leafOperation = new LeafRemove.Remove<B, A, X>(leaf);
            return true;
        }
        else if (listToMerge.isEmpty() && index != parent.size() - 1)
        {
            LeafTier<B, A> next = pool.getLeafTier(mutation.getTxn(), parent.get(index + 1).getAddress());
            levelOfChild.lockAndAdd(next);
            int capacity = next.size() + leaf.size();
            if (capacity <= structure.getLeafSize() + 1)
            {
                listToMerge.add(leaf);
                listToMerge.add(next);
            }
            else
            {
                levelOfChild.unlockAndRemove(next);
            }
        }

        if (listToMerge.isEmpty())
        {
            mutation.leafOperation = new LeafRemove.Remove<B, A, X>(leaf);
        }
        else
        {
            // TODO Test that this activates.
            if (mutation.isDeleting())
            {
                mutation.rewind(2);
                mutation.setDeleting(false);
            }
            LeafTier<B, A> left = listToMerge.get(0);
            LeafTier<B, A> right = listToMerge.get(1);
            levelOfParent.listOfOperations.add(new LeafRemove.Merge<B, A, X>(parent, left, right));
            mutation.leafOperation = new LeafRemove.Remove<B, A, X>(leaf);
        }
        return !listToMerge.isEmpty();
    }

    public final static class Remove<B, A, X>
    implements LeafOperation<B, A, X>
    {
        private final LeafTier<B, A> leaf;

        public Remove(LeafTier<B, A> leaf)
        {
            this.leaf = leaf;
        }

        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
        {
            Structure<B, A, X> structure = mutation.getStructure();
            TierWriter<B, A, X> writer = structure.getWriter();
            
            // TODO Remove single anywhere but far left.
            // TODO Remove single very left most.
            // TODO Remove single very right most.
            int count = 0;
            int found = 0;
            LeafTier<B, A> current = leaf;
            SEARCH: do
            {
                Iterator<B> objects = leaf.iterator();
                while (objects.hasNext())
                {
                    count++;
                    B candidate = objects.next();
                    int compare = mutation.getComparable().compareTo(candidate);
                    if (compare < 0)
                    {
                        break SEARCH;
                    }
                    else if (compare == 0)
                    {
                        found++;
                        if (mutation.deletable.deletable(candidate))
                        {
                            objects.remove();
                            if (count == 1)
                            {
                                if (objects.hasNext())
                                {
                                    mutation.setReplacement(objects.next());
                                }
                                else
                                {
                                    LeafTier<B, A> following = current.getNextAndLock(mutation, levelOfLeaf);
                                    if (following != null)
                                    {
                                        mutation.setReplacement(following.get(0));
                                    }
                                }
                            }
                        }
                        writer.dirty(mutation.getTxn(), current);
                        mutation.setResult(candidate);
                        break SEARCH;
                    }
                }
                current = current.getNextAndLock(mutation, levelOfLeaf);
            }
            while (current != null && mutation.getComparable().compareTo(current.get(0)) == 0);

            if (mutation.getResult() != null
                && count == found
                && current.size() == structure.getLeafSize() - 1
                && mutation.getComparable().compareTo(current.get(current.size() - 1)) == 0)
            {
                for (;;)
                {
                    LeafTier<B, A> subsequent = current.getNextAndLock(mutation, levelOfLeaf);
                    if (subsequent == null || mutation.getComparable().compareTo(subsequent.get(0)) != 0)
                    {
                        break;
                    }
                    current.add(subsequent.remove(0));
                    if (subsequent.size() == 0)
                    {
                        current.setNext(subsequent.getNext());
                        writer.remove(subsequent);
                    }
                    else
                    {
                        writer.dirty(mutation.getTxn(), subsequent);
                    }
                    current = subsequent;
                }
            }

            return mutation.getResult() != null;
        }
    }

    public final static class Fail<B, A, X>
    implements LeafOperation<B, A, X>
    {
        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
        {
            return false;
        }
    }

    public final static class Merge<B, A, X>
    implements Operation<B, A, X>
    {
        private final InnerTier<B, A> parent;

        private final LeafTier<B, A> left;

        private final LeafTier<B, A> right;

        public Merge(InnerTier<B, A> parent, LeafTier<B, A> left, LeafTier<B, A> right)
        {
            this.parent = parent;
            this.left = left;
            this.right = right;
        }

        public void operate(Mutation<B, A, X> mutation)
        {
            parent.remove(parent.getIndex(right.getAddress()));

            while (right.size() != 0)
            {
                left.add(right.remove(0));
            }
            // FIXME Get last leaf. 
            left.setNext(right.getNext());

            TierWriter<B, A, X> writer = mutation.getStructure().getWriter();
            writer.remove(right);
            writer.dirty(mutation.getTxn(), parent);
            writer.dirty(mutation.getTxn(), left);
        }

        public boolean canCancel()
        {
            return true;
        }
    }

    public final static class RemoveLeaf<B, A, X>
    implements Operation<B, A, X>
    {
        private final InnerTier<B, A> parent;

        private final LeafTier<B, A> leaf;

        private final LeafTier<B, A> left;

        public RemoveLeaf(InnerTier<B, A> parent, LeafTier<B, A> leaf, LeafTier<B, A> left)
        {
            this.parent = parent;
            this.leaf = leaf;
            this.left = left;
        }

        public void operate(Mutation<B, A, X> mutation)
        {
            parent.remove(parent.getIndex(leaf.getAddress()));

            left.setNext(leaf.getNext());

            TierWriter<B, A, X> writer = mutation.getStructure().getWriter();
            writer.remove(leaf);
            writer.dirty(mutation.getTxn(), parent);
            writer.dirty(mutation.getTxn(), left);

            mutation.setOnlyChild(false);
        }

        public boolean canCancel()
        {
            return true;
        }
    }
}