package com.goodworkalan.strata;

/**
 * Determine if the leaf that will hold the inserted value is full and ready to
 * split, if it is full and part of linked list of b+tree leaves of duplicate
 * index values, or it it can be inserted without splitting.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class HowToInertLeaf<T, A>
implements Decision<T, A> {
    /**
     * Create a new decision.
     */
    public HowToInertLeaf() {
    }

    /**
     * Determine if the leaf that will hold the inserted value is full and ready
     * to split, if it is full and part of linked list of b+tree leaves of
     * duplicate index values, or it it can be inserted without splitting.
     * 
     * @param mutation
     *            The mutation state container.
     * @param parentLevel
     *            The operations to perform on the parent tier.
     * @param childLevel
     *            The operations to perform on the child tier.
     * @param parent
     *            The parent tier.
     * @return True if this decision added an operation to split the leaf.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> parentLevel, Level<T, A> childLevel, Tier<T, A> parent) {
        // Get the collection of the core services of the b+tree.
        Structure<T, A> structure = mutation.getStructure();

        // By default, return false.
        boolean split = true;

        // Find the branch that navigates to the leaf child.
        int branch = parent.find(mutation.getComparable());
        Tier<T, A> leaf = structure.getStorage().load(mutation.getStash(), parent.getChildAddress(branch));

        // Lock the child level exclusively.
        childLevel.locker = new WriteLockExtractor();
        childLevel.lockAndAdd(leaf);

        // If the leaf size is equal to the maximum leaf size, then we either
        // have a leaf that must split or a leaf that is full of objects that
        // have the same index value. Otherwise, we have a leaf that has a free
        // slot.

        if (leaf.getSize() == structure.getLeafSize()) {
            // If the index value of the first value is equal to the index value
            // of the last value, then we have a linked list of duplicate index
            // values. Otherwise, we have a full page that can split.

            Comparable<? super T> first = mutation.getStructure().getComparableFactory().newComparable(mutation.getStash(), leaf.getRecord(0));
            if (first.compareTo(leaf.getRecord(leaf.getSize() - 1)) == 0) {
                // If the inserted value is less than the current value, create
                // a new page to the left of the leaf, if it is greater create a
                // new page to the right of the leaf. If it is equal, append the
                // leaf to the linked list of duplicate index values.

                int compare = mutation.getComparable().compareTo(leaf.getRecord(0));
                if (compare < 0) {
                    mutation.leafOperation = new SplitLinkedListLeft<T, A>(parent);
                } else if (compare > 0) {
                    mutation.leafOperation = new SplitLinkedListRight<T, A>(parent);
                } else {
                    mutation.leafOperation = new InsertLinkedList<T, A>(leaf);
                    split = false;
                }
            } else {
                // Insert the value and then split the leaf.
                parentLevel.operations.add(new SplitLeaf<T, A>(parent));
                mutation.leafOperation = new InsertSorted<T, A>(parent);
            }
        } else {
            // No split and the value is inserted into leaf.
            mutation.leafOperation = new InsertSorted<T, A>(parent);
            split = false;
        }

        // Let the caller know if we've added a split operation.
        return split;
    }
}
