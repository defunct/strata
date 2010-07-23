package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.getNextAndLock;

// TODO Document.
public final class RemoveObject<T, A>
implements LeafOperation<T, A> {
    // TODO Document.
    private final Tier<T, A> leaf;

    // TODO Document.
    public RemoveObject(Tier<T, A> leaf) {
        this.leaf = leaf;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf) {
        Structure<T, A> structure = mutation.getStructure();
        Stage<T, A> writer = structure.getStage();
        
        // TODO Remove single anywhere but far left.
        // TODO Remove single very left most.
        // TODO Remove single very right most.
        int count = 0;
        int found = 0;
        Tier<T, A> current = leaf;
        SEARCH: do {
            for (int i = 0, stop = leaf.getSize(); i < stop; i++) {
                count++;
                T candidate = leaf.getRecord(i);
                int compare = mutation.getComparable().compareTo(candidate);
                if (compare < 0) {
                    break SEARCH;
                } else if (compare == 0) {
                    found++;
                    if (mutation.deletable.deletable(candidate)) {
                        leaf.clear(i, 1);
                        if (count == 1) {
                            if (leaf.getSize() != 0) {
                                mutation.setReplacement(leaf.getRecord(0));
                            } else {
                                // When would this ever happen?
                                Tier<T, A> following = getNextAndLock(mutation, current, levelOfLeaf);
                                if (following != null) {
                                    mutation.setReplacement(following.getRecord(0));
                                }
                            }
                        }
                    }
                    writer.dirty(mutation.getStash(), current);
                    mutation.setResult(candidate);
                    break SEARCH;
                }
            }
            current = getNextAndLock(mutation, current, levelOfLeaf);
        }
        while (current != null && mutation.getComparable().compareTo(current.getRecord(0)) == 0);

        if (mutation.getResult() != null
            && count == found
            && current.getSize() == structure.getLeafSize() - 1
            && mutation.getComparable().compareTo(current.getRecord(current.getSize() - 1)) == 0)
        {
            for (;;) {
                Tier<T, A> subsequent = getNextAndLock(mutation, current, levelOfLeaf);
                if (subsequent == null || mutation.getComparable().compareTo(subsequent.getRecord(0)) != 0) {
                    break;
                }
                current.addRecord(current.getSize(), subsequent.getRecord(0));
                subsequent.clear(0, 1);
                if (subsequent.getSize() == 0) {
                    current.setNext(subsequent.getNext());
                    writer.free(mutation.getStash(), subsequent);
                } else {
                    writer.dirty(mutation.getStash(), subsequent);
                }
                current = subsequent;
            }
        }

        return mutation.getResult() != null;
    }
}