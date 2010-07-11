package com.goodworkalan.strata;

/**
 * An leaf level of the b-tree that contains object values.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class LeafTier<T, A>
extends Tier<T, A> {
    /** The serial version id. */
    private static final long serialVersionUID = 1L;

    /** The address of the next leaf in the b-tree. */
    private A next;

    /**
     * Get the index of the first value object for the given comparable
     * representing the value according to the b-tree order. If there is no such
     * object in the leaf, return the index of the insert location.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return The index of the first value object or the insert location of the
     *         value object.
     */
    public int find(Comparable<? super T> comparable) {
        int low = 1;
        int high = size() - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            int compare = comparable.compareTo(get(mid));
            if (compare > 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if (low < size()) {
            while (low != 0 && comparable.compareTo(get(low - 1)) == 0) {
                low--;
            }
            return low;
        }
        return low - 1;
    }

    /**
     * Get address of the next leaf in the b-tree or null if this is the last
     * leaf in the b-tree.
     * 
     * @return The address of the next leaf or null.
     */
    public A getNext() {
        return next;
    }

    /**
     * Set the address the next leaf in the b-tree.
     * 
     * @param next
     *            The address of the next leaf or null if this is the last leaf
     *            in the b-tree.
     */
    public void setNext(A next) {
        this.next = next;
    }
}