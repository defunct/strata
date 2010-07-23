package com.goodworkalan.strata;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Read and write a tiers.
 * 
 * @author Alan Gutierrez
 */
public abstract class Tier<Record, Address> {
    /**
     * The read write lock for the underlying data. This is here, because we are
     * really locking the underlying data.
     */
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
    /**
     * Get the address of this tier.
     * 
     * @return The address.
     */
    public abstract Address getAddress();

    /**
     * Get whether the child tier is a leaf.
     * 
     * @return True if the child tier is a leaf.
     */
    public abstract boolean isChildLeaf();

    /**
     * Set whether the child tier is a leaf.
     * 
     * @param leaf
     *            True if the child tier is a leaf.
     */
    public abstract void setChildLeaf(boolean leaf);

    /**
     * Get address of the next leaf in the b-tree or null if this is the last
     * leaf in the b-tree.
     * 
     * @return The address of the next leaf or null.
     */
    public abstract Address getNext();

    /**
     * Set the address the next leaf in the b-tree.
     * 
     * @param next
     *            The address of the next leaf or null if this is the last leaf
     *            in the b-tree.
     */
    public abstract void setNext(Address next);
    
    /**
     * Read a record from the tier cassette.
     * 
     * @param index
     *            The index of the record.
     */
    public abstract Record getRecord(int index);
    
    /**
     * Read a child address from the tier cassette.
     * 
     * @param index
     *            The index of the child address.
     */
    public abstract Address getChildAddress(int index);
    
    /**
     * Add a record and branch to the tier cassette. If index is the 
     * size of the tier, the tier is extended.
     */
    public abstract void addBranch(int index, Record record, Address address);
        
    /**
     * Add a leaf to the tier cassette.
     */
    public abstract void addRecord(int index, Record record);
    
    public abstract void setRecord(int index, Record record);
    
    /**
     * Get the size of the cassette in records.
     * 
     * @return The size.
     */
    public abstract int getSize();

    /**
     * Remove the records and branch address from the given index for the given
     * count of records.
     * <p>
     * The count of children addresses will always either be zero or the same as
     * the count of records, so that <code>Tier</code> implementations do not have
     * to worry about different counts for records and children.
     *  
     * @param offset
     *            The index of the record.
     * @param count
     *            The count of records.
     */
    public abstract void clear(int start, int count);
    
    
    /**
     * Get the index of the branch with the given child tier address.
     * 
     * @param address
     *            The child tier address.
     * @return The index of the branch with the given child tier address or
     *         <code>-1</code> if not found.
     */
    public int getIndexOfChildAddress(Address address) {
        for (int i = 0, stop = getSize(); i < stop; i++) {
            if (getChildAddress(i).equals(address)) {
                return i;
            }
        }
        return -1;
    }
    
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
    public int find(Comparable<? super Record> comparable) {
        int low = 1;
        int high = getSize() - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            int compare = comparable.compareTo(getRecord(mid));
            if (compare > 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if (low < getSize()) {
            while (low != 0 && comparable.compareTo(getRecord(low - 1)) == 0) {
                low--;
            }
            return low;
        }
        return low - 1;
    }
}
