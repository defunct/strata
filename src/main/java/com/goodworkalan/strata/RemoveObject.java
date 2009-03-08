package com.goodworkalan.strata;

import java.util.Iterator;

public final class RemoveObject<T, A>
implements LeafOperation<T, A>
{
    // TODO Document.
    private final LeafTier<T, A> leaf;

    // TODO Document.
    public RemoveObject(LeafTier<T, A> leaf)
    {
        this.leaf = leaf;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();
        Allocator<T, A> writer = structure.getAllocator();
        
        // TODO Remove single anywhere but far left.
        // TODO Remove single very left most.
        // TODO Remove single very right most.
        int count = 0;
        int found = 0;
        LeafTier<T, A> current = leaf;
        SEARCH: do
        {
            Iterator<T> objects = leaf.iterator();
            while (objects.hasNext())
            {
                count++;
                T candidate = objects.next();
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
                                LeafTier<T, A> following = current.getNextAndLock(mutation, levelOfLeaf);
                                if (following != null)
                                {
                                    mutation.setReplacement(following.get(0));
                                }
                            }
                        }
                    }
                    writer.dirty(mutation.getStash(), current);
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
                LeafTier<T, A> subsequent = current.getNextAndLock(mutation, levelOfLeaf);
                if (subsequent == null || mutation.getComparable().compareTo(subsequent.get(0)) != 0)
                {
                    break;
                }
                current.add(subsequent.remove(0));
                if (subsequent.size() == 0)
                {
                    current.setNext(subsequent.getNext());
                    writer.remove(mutation.getStash(), subsequent);
                }
                else
                {
                    writer.dirty(mutation.getStash(), subsequent);
                }
                current = subsequent;
            }
        }

        return mutation.getResult() != null;
    }
}