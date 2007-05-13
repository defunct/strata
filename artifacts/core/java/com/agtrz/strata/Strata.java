package com.agtrz.strata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Strata
implements Serializable
{
    public final static short INNER = 1;

    public final static short LEAF = 2;

    private static final long serialVersionUID = 20070207L;

    private final Structure structure;

    private final Object rootKey;

    private int size;

    public Strata()
    {
        this(new ArrayListStorage(), null, new ComparableExtractor(), false, 5);
    }

    public Strata(Storage storage, Object txn, FieldExtractor extractor, boolean cacheFields, int size)
    {
        Cooper cooper = cacheFields ? (Cooper) new BucketCooper() : (Cooper) new LookupCooper();
        Structure structure = new Structure(storage, extractor, cooper, size);
        InnerTier root = structure.getStorage().newInnerTier(structure, txn, LEAF);
        LeafTier leaf = structure.getStorage().newLeafTier(structure, txn);
        leaf.write(txn);

        root.add(new Branch(leaf.getKey(), null, 0));
        root.write(txn);

        this.structure = structure;
        this.rootKey = root.getKey();
    }

    public int getSize()
    {
        return size;
    }

    public Query query(Object txn)
    {
        return new Query(txn);
    }

    public final static class Creator
    {
        private FieldExtractor extractor = new ComparableExtractor();

        private Storage storage = new ArrayListStorage();

        private int size = 5;

        private boolean cacheFields = false;

        public Strata create(Object txn)
        {
            return new Strata(storage, txn, extractor, cacheFields, size);
        }

        public void setStorage(Storage storage)
        {
            this.storage = storage;
        }

        public void setFieldExtractor(FieldExtractor extractor)
        {
            this.extractor = extractor;
        }

        public void setCacheFields(boolean cacheFields)
        {
            this.cacheFields = cacheFields;
        }

        public void setSize(int size)
        {
            this.size = size;
        }
    }

    public static class Structure
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Storage storage;

        private final int size;

        private final FieldExtractor extractor;

        private final Cooper cooper;

        public Structure(Storage storage, FieldExtractor extractor, Cooper cooper, int size)
        {
            this.storage = storage;
            this.extractor = extractor;
            this.cooper = cooper;
            this.size = size;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public int getSize()
        {
            return size;
        }

        public FieldExtractor getFieldExtractor()
        {
            return extractor;
        }

        public Comparable[] getFields(Object txn, Object object)
        {
            return cooper.getFields(txn, extractor, object);
        }

        public Object newBucket(Comparable[] fields, Object keyOfObject)
        {
            return cooper.newBucket(fields, keyOfObject);
        }

        public Object newBucket(Object txn, Object keyOfObject)
        {
            return cooper.newBucket(txn, extractor, keyOfObject);
        }

        public Object getObjectKey(Object object)
        {
            return cooper.getObjectKey(object);
        }
    }

    public interface FieldExtractor
    {
        public Comparable[] getFields(Object txn, Object object);
    }

    public final static class ComparableExtractor
    implements FieldExtractor, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public Comparable[] getFields(Object txn, Object object)
        {
            return new Comparable[] { (Comparable) object };
        }
    }

    public final static class CopaceticComparator
    implements Comparator
    {
        private final Object txn;

        private final Structure structure;

        public CopaceticComparator(Object txn, Structure structure)
        {
            this.txn = txn;
            this.structure = structure;
        }

        public int compare(Object left, Object right)
        {
            return Strata.compare(structure.getFields(txn, left), structure.getFields(txn, right));
        }
    }

    private interface Cooper
    {
        public Object newBucket(Object txn, FieldExtractor fields, Object keyOfObject);

        public Object newBucket(Comparable[] fields, Object keyOfObject);

        public Object getObjectKey(Object object);

        public Comparable[] getFields(Object txn, FieldExtractor extractor, Object object);
    }

    private final class Bucket
    {
        public final Comparable[] fields;

        public final Object objectKey;

        public Bucket(Comparable[] fields, Object objectKey)
        {
            this.fields = fields;
            this.objectKey = objectKey;
        }
    }

    public class BucketCooper
    implements Cooper, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public Object newBucket(Object txn, FieldExtractor extractor, Object keyOfObject)
        {
            return new Bucket(extractor.getFields(txn, keyOfObject), keyOfObject);
        }

        public Object newBucket(Comparable[] fields, Object keyOfObject)
        {
            return new Bucket(fields, keyOfObject);
        }

        public Comparable[] getFields(Object txn, FieldExtractor extractor, Object object)
        {
            return ((Bucket) object).fields;
        }

        public Object getObjectKey(Object object)
        {
            return ((Bucket) object).objectKey;
        }
    }

    public final class LookupCooper
    implements Cooper, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public Object newBucket(Object txn, FieldExtractor extractor, Object keyOfObject)
        {
            return keyOfObject;
        }

        public Object newBucket(Comparable[] comparables, Object keyOfObject)
        {
            return keyOfObject;
        }

        public Comparable[] getFields(Object txn, FieldExtractor extractor, Object object)
        {
            return extractor.getFields(txn, object);
        }

        public Object getObjectKey(Object object)
        {
            return object;
        }
    }

    public final static class Split
    {
        private final Object pivot;

        private final Tier right;

        public Split(Object pivot, Tier right)
        {
            this.pivot = pivot;
            this.right = right;
        }

        public Object getPivot()
        {
            return pivot;
        }

        public Tier getRight()
        {
            return right;
        }
    }

    public final class TierSet
    {
        private final Map mapOfTiers;

        public TierSet(Map mapOfTiers)
        {
            this.mapOfTiers = mapOfTiers;
        }

        public void add(Tier tier)
        {
            mapOfTiers.put(tier.getKey(), tier);
        }
    }

    public interface Tier
    {
        public Object getKey();

        public Object getStorageData();

        /**
         * Return true if this tier is full.
         * 
         * @return True if the tier is full.
         */
        public boolean isFull();

        /**
         * Return the number of objects or objects used to pivot in this tier.
         * For an inner tier the size is the number of objects, while the
         * number of child tiers is the size plus one.
         * 
         * @return The size of the tier.
         */
        public int getSize();

        public Split split(Object txn, Comparable[] fields, Object keyOfObject, TierSet setOfDirty);

        /**
         * Merge the contents of a tier to the left of this tier into this
         * tier.
         * 
         * @param txn
         *            The transaction of the query.
         * @param left
         *            The tier to the left of this tier.
         */
        public void consume(Object txn, Tier left, TierSet setOfDirty);

        public void write(Object txn);

        public void copacetic(Object txn, Copacetic copacetic);
    }

    public final static class LeafTier
    implements Strata.Tier
    {
        private Object keyOfNext;

        private Object keyOfPrevious;

        private final Object storageData;

        private final List listOfObjects;

        private final Strata.Structure structure;

        public LeafTier(Strata.Structure structure, Object storageData)
        {
            this.structure = structure;
            this.storageData = storageData;
            this.listOfObjects = new ArrayList(structure.getSize());
        }

        public Object getKey()
        {
            return structure.getStorage().getKey(this);
        }

        public int getSize()
        {
            return listOfObjects.size();
        }

        public Object getStorageData()
        {
            return storageData;
        }

        public boolean isFull()
        {
            return getSize() == structure.getSize();
        }

        public void setNextLeafKey(Object nextLeafKey)
        {
            this.keyOfNext = nextLeafKey;
        }

        public Object getNextLeafKey()
        {
            return keyOfNext;
        }

        public void setPreviousLeafKey(Object previousLeafKey)
        {
            this.keyOfPrevious = previousLeafKey;
        }

        public Object getPreviousLeafKey()
        {
            return keyOfPrevious;
        }

        public ListIterator listIterator()
        {
            return listOfObjects.listIterator();
        }

        public void shift(Object object)
        {
            listOfObjects.add(0, object);
        }

        public void add(Object object)
        {
            listOfObjects.add(object);
        }

        public Object remove(int index)
        {
            return listOfObjects.remove(index);
        }

        public Object get(int index)
        {
            return listOfObjects.get(index);
        }

        private LeafTier getNext(Object txn)
        {
            return structure.getStorage().getLeafTier(structure, txn, getNextLeafKey());
        }

        private boolean endOfList(Object txn, Comparable[] fields, LeafTier last)
        {
            return structure.getStorage().isKeyNull(last.getNextLeafKey()) || compare(structure.getFields(txn, last.getNext(txn).get(0)), fields) != 0;
        }

        private void link(Object txn, LeafTier leaf, LeafTier nextLeaf, Strata.TierSet setOfDirty)
        {
            setOfDirty.add(leaf);
            setOfDirty.add(nextLeaf);
            Object nextLeafKey = leaf.getNextLeafKey();
            leaf.setNextLeafKey(nextLeaf.getKey());
            nextLeaf.setNextLeafKey(nextLeafKey);
            if (!structure.getStorage().isKeyNull(nextLeafKey))
            {
                LeafTier next = nextLeaf.getNext(txn);
                setOfDirty.add(next);
                next.setPreviousLeafKey(nextLeaf.getKey());
            }
            nextLeaf.setPreviousLeafKey(leaf.getKey());
        }

        public Split split(Object txn, Comparable[] fields, Object keyOfObject, TierSet setOfDirty)
        {
            if (!isFull())
            {
                throw new IllegalStateException();
            }

            int middle = structure.getSize() >> 1;
            boolean odd = (structure.getSize() & 1) == 1;
            int lesser = middle - 1;
            int greater = odd ? middle + 1 : middle;

            int partition = -1;

            Comparable[] candidate = structure.getFields(txn, get(middle));
            for (int i = 0; partition == -1 && i < middle; i++)
            {
                if (compare(candidate, structure.getFields(txn, get(lesser))) != 0)
                {
                    partition = lesser + 1;
                }
                else if (compare(candidate, structure.getFields(txn, get(greater))) != 0)
                {
                    partition = greater;
                }
                lesser--;
                greater++;
            }

            Storage storage = structure.getStorage();
            Split split = null;
            if (partition == -1)
            {
                Comparable[] repeated = structure.getFields(txn, get(0));
                int compare = compare(fields, repeated);
                if (compare < 0)
                {
                    LeafTier right = storage.newLeafTier(structure, txn);
                    while (getSize() != 0)
                    {
                        right.add(remove(0));
                    }

                    link(txn, this, right, setOfDirty);

                    split = new Split(structure.newBucket(fields, keyOfObject), right);
                }
                else if (compare > 0)
                {
                    LeafTier last = this;
                    while (!endOfList(txn, repeated, last))
                    {
                        last = last.getNext(txn);
                    }

                    LeafTier right = storage.newLeafTier(structure, txn);
                    link(txn, last, right, setOfDirty);

                    split = new Split(get(0), right);
                }
            }
            else
            {
                LeafTier right = storage.newLeafTier(structure, txn);

                while (partition != getSize())
                {
                    right.add(remove(partition));
                }

                link(txn, this, right, setOfDirty);

                split = new Split(get(getSize() - 1), right);
            }

            return split;
        }

        private void ensureNextLeafTier(Object txn, Comparable[] fields, Strata.TierSet setOfDirty)
        {
            Strata.Storage storage = structure.getStorage();
            if (storage.isKeyNull(getNextLeafKey()) || compare(fields, structure.getFields(txn, getNext(txn).get(0))) != 0)
            {
                LeafTier nextLeaf = storage.newLeafTier(structure, txn);
                link(txn, this, nextLeaf, setOfDirty);
            }
        }

        public void append(Object txn, Comparable[] fields, Object keyOfObject, TierSet setOfDirty)
        {
            if (getSize() == structure.getSize())
            {
                ensureNextLeafTier(txn, fields, setOfDirty);
                getNext(txn).append(txn, fields, keyOfObject, setOfDirty);
            }
            else
            {
                add(structure.newBucket(fields, keyOfObject));
                setOfDirty.add(this);
            }
        }

        public void insert(Object txn, Comparable[] fields, Object keyOfObject, TierSet setOfDirty)
        {
            if (getSize() == structure.getSize())
            {
                ensureNextLeafTier(txn, fields, setOfDirty);
                getNext(txn).append(txn, fields, keyOfObject, setOfDirty);
            }
            else
            {
                Object bucket = structure.newBucket(fields, keyOfObject);

                ListIterator objects = listIterator();
                while (objects.hasNext())
                {
                    Object before = objects.next();
                    if (compare(structure.getFields(txn, before), fields) > 0)
                    {
                        objects.previous();
                        objects.add(bucket);
                        break;
                    }
                }

                if (!objects.hasNext())
                {
                    add(bucket);
                }

                setOfDirty.add(this);
            }
        }

        public Cursor find(Object txn, Comparable[] fields)
        {
            for (int i = 0; i < getSize(); i++)
            {
                int compare = compare(structure.getFields(txn, get(i)), fields);
                if (compare >= 0)
                {
                    return new Cursor(structure, txn, this, i);
                }
            }
            return new Cursor(structure, txn, this, getSize());
        }

        /**
         * Remove all objects that match the object according to the equator,
         * while updating the inner tree if the object removed was used as a
         * pivot in the inner tree.
         * 
         * @param criteria
         *            An object that represents the objects that will be
         *            removed from the B+Tree.
         * @param equator
         *            The comparison logic that will determine of an object in
         *            the leaf is equal to the specified object.
         * @return A colletion of the objects removed from the tree.
         */
        public Collection remove(Object txn, Object keyOfObject)
        {
            List listOfRemoved = new ArrayList();
            Iterator objects = listIterator();
            while (objects.hasNext())
            {
                Object candidate = objects.next();
                if (keyOfObject.equals(structure.getObjectKey(candidate)))
                {
                    listOfRemoved.add(candidate);
                    objects.remove();
                }
            }
            Strata.Storage storage = structure.getStorage();
            LeafTier lastLeaf = this;
            LeafTier leaf = null;
            while (!storage.isKeyNull(lastLeaf.getNextLeafKey()) && keyOfObject.equals(structure.getObjectKey((leaf = lastLeaf.getNext(txn)).get(0))))
            {
                objects = leaf.listIterator();
                while (objects.hasNext())
                {
                    Object candidate = structure.getObjectKey(objects.next());
                    if (keyOfObject.equals(candidate))
                    {
                        listOfRemoved.add(candidate);
                        objects.remove();
                    }
                }
                if (leaf.getSize() == 0)
                {
                    lastLeaf.setNextLeafKey(leaf.getNextLeafKey());
                    leaf.getNext(txn).setPreviousLeafKey(lastLeaf.getKey());
                }
                lastLeaf = leaf;
            }
            return listOfRemoved;
        }

        private LeafTier getPrevious(Object txn)
        {
            return structure.getStorage().getLeafTier(structure, txn, getPreviousLeafKey());
        }

        public void consume(Object txn, Tier left, TierSet setOfDirty)
        {
            LeafTier leaf = (LeafTier) left;

            setPreviousLeafKey(leaf.getPreviousLeafKey());
            if (!structure.getStorage().isKeyNull(getPreviousLeafKey()))
            {
                getPrevious(txn).setNextLeafKey(getKey());
            }

            while (leaf.getSize() != 0)
            {
                shift(leaf.remove(leaf.getSize() - 1));
            }

            setOfDirty.add(this);

            structure.getStorage().free(structure, txn, leaf);
        }

        public void write(Object txn)
        {
            structure.getStorage().write(structure, txn, this);
        }

        public String toString()
        {
            return listOfObjects.toString();
        }

        public void copacetic(Object txn, Copacetic copacetic)
        {
            if (getSize() < 1)
            {
                throw new IllegalStateException();
            }
            Object previous = null;
            Iterator objects = listIterator();
            Comparator comparator = new CopaceticComparator(txn, structure);
            while (objects.hasNext())
            {
                Object object = objects.next();
                if (previous != null && comparator.compare(previous, object) > 0)
                {
                    throw new IllegalStateException();
                }
                previous = object;
            }
            if (!structure.getStorage().isKeyNull(getNextLeafKey()) && comparator.compare(get(getSize() - 1), getNext(txn).get(0)) == 0 && structure.getSize() != getSize() && comparator.compare(get(0), get(getSize() - 1)) != 0)
            {
                throw new IllegalStateException();
            }
        }

    }

    public final static class Branch
    {
        private final Object leftKey;

        private final Object object;

        private int size;

        public Branch(Object keyOfLeft, Object object, int size)
        {
            this.leftKey = keyOfLeft;
            this.object = object;
            this.size = size;
        }

        public Object getLeftKey()
        {
            return leftKey;
        }

        public Object getObject()
        {
            return object;
        }

        public int getSize()
        {
            return size;
        }

        public void setSize(int size)
        {
            this.size = size;
        }

        public boolean isTerminal()
        {
            return object == null;
        }

        public String toString()
        {
            return object == null ? "TERMINAL" : object.toString();
        }
    }

    public final static class InnerTier
    implements Tier
    {
        protected final Structure structure;

        private final Object key;

        private short childType;

        private final List listOfBranches;

        public InnerTier(Structure structure, Object key, short typeOfChildren)
        {
            this.structure = structure;
            this.key = key;
            this.childType = typeOfChildren;
            this.listOfBranches = new ArrayList(structure.getSize() + 1);
        }

        public Object getKey()
        {
            return structure.getStorage().getKey(this);
        }

        public Object getStorageData()
        {
            return key;
        }

        public short getChildType()
        {
            return childType;
        }

        public int getSize()
        {
            return listOfBranches.size() - 1;
        }

        public void setChildType(short childType)
        {
            this.childType = childType;
        }

        public void add(Object txn, Object keyOfLeft, Object keyOfObject, int sizeOfTier)
        {
            Object bucket = null;
            if (keyOfObject != null)
            {
                bucket = structure.newBucket(txn, keyOfObject);
            }
            listOfBranches.add(new Branch(keyOfLeft, bucket, sizeOfTier));
        }

        public void add(Branch branch)
        {
            listOfBranches.add(branch);
        }

        public void shift(Branch branch)
        {
            listOfBranches.add(0, branch);
        }

        public Branch remove(int index)
        {
            return (Branch) listOfBranches.remove(index);
        }

        public ListIterator listIterator()
        {
            return listOfBranches.listIterator();
        }

        public Branch get(int index)
        {
            return (Branch) listOfBranches.get(index);
        }

        public Split split(Object txn, Comparable[] fields, Object keyOfObject, TierSet setOfDirty)
        {
            int partition = (structure.getSize() + 1) / 2;

            Storage storage = structure.getStorage();
            InnerTier right = storage.newInnerTier(structure, txn, getChildType());
            for (int i = partition; i < structure.getSize() + 1; i++)
            {
                right.add(remove(partition));
            }

            Branch branch = remove(getSize());
            Object pivot = branch.getObject();
            add(new Branch(branch.getLeftKey(), null, branch.getSize()));

            setOfDirty.add(this);
            setOfDirty.add(right);

            return new Strata.Split(pivot, right);
        }

        public boolean isFull()
        {
            return getSize() == structure.getSize();
        }

        public Branch find(Object txn, Comparable[] fields)
        {
            Iterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.isTerminal() || compare(fields, structure.getFields(txn, branch.getObject())) <= 0)
                {
                    return branch;
                }
            }
            throw new IllegalStateException();
        }

        public int getIndexOfTier(Object keyOfTier)
        {
            int index = 0;
            Iterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.getLeftKey().equals(keyOfTier))
                {
                    return index;
                }
                index++;
            }
            return -1;
        }

        public Object removeLeafTier(Object keyOfLeafTier, TierSet setOfDirty)
        {
            Branch previous = null;
            ListIterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.getLeftKey().equals(keyOfLeafTier))
                {
                    branches.remove();
                    setOfDirty.add(this);
                    break;
                }
                previous = branch;
            }
            return previous.getObject();
        }

        public void replacePivot(Object txn, Comparable[] oldPivot, Object newPivot, TierSet setOfDirty)
        {
            ListIterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (compare(oldPivot, structure.getFields(txn, branch.getObject())) == 0)
                {
                    branches.set(new Branch(branch.getLeftKey(), newPivot, branch.getSize()));
                    setOfDirty.add(this);
                    break;
                }
            }
        }

        public final void splitRootTier(Object txn, Split split, TierSet setOfDirty)
        {
            Strata.Storage storage = structure.getStorage();
            InnerTier left = storage.newInnerTier(structure, txn, getChildType());
            int count = getSize() + 1;
            for (int i = 0; i < count; i++)
            {
                left.add(remove(0));
            }
            setOfDirty.add(left);

            add(new Branch(left.getKey(), split.getPivot(), left.getSize()));
            add(new Branch(split.getRight().getKey(), null, split.getRight().getSize()));
            setChildType(INNER);

            setOfDirty.add(this);
        }

        public void replace(Tier tier, Split split, TierSet setOfDirty)
        {
            Object keyOfTier = tier.getKey();
            ListIterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.getLeftKey().equals(keyOfTier))
                {
                    branches.set(new Branch(keyOfTier, split.getPivot(), tier.getSize()));
                    branches.add(new Branch(split.getRight().getKey(), branch.getObject(), split.getRight().getSize()));
                    setOfDirty.add(this);
                    break;
                }
            }
        }

        public boolean isLeaf()
        {
            return false;
        }

        public boolean canMerge(Tier tier)
        {
            int index = getIndexOfTier(tier);
            if (index > 0 && get(index - 1).getSize() + get(index).getSize() <= structure.getSize())
            {
                return true;
            }
            else if (index <= structure.getSize() && get(index).getSize() + get(index + 1).getSize() <= structure.getSize())
            {
                return true;

            }
            return false;
        }

        public void merge(Object txn, Tier tier, TierSet setOfDirty)
        {
            int index = getIndexOfTier(tier.getKey());
            if (canMerge(index - 1, index))
            {
                merge(txn, index - 1, index, setOfDirty);
            }
            else if (canMerge(index, index + 1))
            {
                merge(txn, index, index + 1, setOfDirty);
            }
        }

        public void consume(Object txn, Tier left, TierSet setOfDirty)
        {
            InnerTier inner = (InnerTier) left;

            Branch oldPivot = inner.get(inner.getSize());
            shift(new Branch(oldPivot.getLeftKey(), oldPivot.getObject(), oldPivot.getSize()));

            for (int i = left.getSize(); i > 0; i--)
            {
                shift(inner.get(i));
            }

            setOfDirty.add(this);
            structure.getStorage().free(structure, txn, inner);
        }

        public void write(Object txn)
        {
            structure.getStorage().write(structure, txn, this);
        }

        public void copacetic(Object txn, Copacetic copacetic)
        {
            if (getSize() < 0)
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Object lastLeftmost = null;

            Comparator comparator = new CopaceticComparator(txn, structure);
            Iterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (!branches.hasNext() && !branch.isTerminal())
                {
                    throw new IllegalStateException();
                }

                Strata.Tier left = getTier(txn, branch.getLeftKey());
                left.copacetic(txn, copacetic);

                if (branch.getSize() < left.getSize())
                {
                    throw new IllegalStateException();
                }

                if (!branch.isTerminal())
                {
                    // Each key must be less than the one next to it.
                    if (previous != null && comparator.compare(previous, branch.getObject()) >= 0)
                    {
                        throw new IllegalStateException();
                    }

                    // Each key must occur only once in the inner tiers.
                    if (!copacetic.unique(branch.getObject()))
                    {
                        throw new IllegalStateException();
                    }
                }
                previous = branch.getObject();

                Object leftmost = getLeftMost(txn, left, getChildType());
                if (lastLeftmost != null && comparator.compare(lastLeftmost, leftmost) >= 0)
                {
                    throw new IllegalStateException();
                }
                lastLeftmost = leftmost;
            }
        }

        private Object getLeftMost(Object txn, Tier tier, short childType)
        {
            while (childType != LEAF)
            {
                InnerTier inner = (InnerTier) tier;
                childType = inner.getChildType();
                tier = inner.getTier(txn, inner.get(0).getLeftKey());
            }
            LeafTier leaf = (LeafTier) tier;
            return leaf.get(0);
        }

        // FIXME Is this correct for the inner tier? Do I count pivots or
        // branches?
        private boolean canMerge(int indexOfLeft, int indexOfRight)
        {
            return indexOfLeft >= 0 && indexOfRight <= getSize() && get(indexOfLeft).getSize() + get(indexOfRight).getSize() < structure.getSize() + 1;
        }

        public Tier getTier(Object txn, Object key)
        {
            if (getChildType() == INNER)
                return structure.getStorage().getInnerTier(structure, txn, key);
            return structure.getStorage().getLeafTier(structure, txn, key);
        }

        private void merge(Object txn, int indexOfLeft, int indexOfRight, TierSet setOfDirty)
        {
            Strata.Tier left = getTier(txn, get(indexOfLeft).getLeftKey());
            Strata.Tier right = getTier(txn, get(indexOfRight).getLeftKey());

            right.consume(txn, left, setOfDirty);

            get(indexOfRight).setSize(right.getSize());
            remove(indexOfLeft);

            setOfDirty.add(this);
        }

        public String toString()
        {
            return listOfBranches.toString();
        }
    }

    public interface Storage
    extends Serializable
    {
        public InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren);

        public LeafTier newLeafTier(Structure structure, Object txn);

        public LeafTier getLeafTier(Structure structure, Object txn, Object key);

        public InnerTier getInnerTier(Structure structure, Object txn, Object key);

        public void write(Structure structure, Object txn, InnerTier inner);

        public void write(Structure structure, Object txn, LeafTier leaf);

        public void free(Structure structure, Object txn, InnerTier inner);

        public void free(Structure structure, Object txn, LeafTier leaf);

        public Object getKey(Strata.Tier leaf);

        public Object getNullKey();

        public boolean isKeyNull(Object object);
    }
    
    private interface Operation
    {
        public void operate();
    }

    public final class Query
    {
        private final Object txn;

        private final Map mapOfDirtyTiers;

        public Query(Object txn)
        {
            this.txn = txn;
            this.mapOfDirtyTiers = new HashMap();
        }

        public int getSize()
        {
            return size;
        }

        public void insert(Object keyOfObject)
        {
            Comparable[] fields = structure.getFieldExtractor().getFields(txn, keyOfObject);

            // Maintaining a list of tiers to split.
            //
            // During the decent into the tree, we check for full tiers. When
            // a full tier is first encountered, a list of full tiers is begun
            // starting with the parent of the first full tier, and followed
            // by all of the full tiers to the leaf tier. If less than full
            // tier is encountered, we reset the list of tiers.
            //
            // The list of tiers is locked in order from upper most teirs to
            // the leaf.

            List listOfFullTiers = new ArrayList();
            InnerTier grandParent = null;
            InnerTier parent = null;
            InnerTier inner = getRoot();

            if (inner.isFull())
            {
                listOfFullTiers.add(grandParent);
                listOfFullTiers.add(parent);
            }

            TierSet setOfDirty = new TierSet(mapOfDirtyTiers);
            for (;;)
            {
                grandParent = parent;
                parent = inner;
                Branch branch = parent.find(txn, fields);
                Tier tier = parent.getTier(txn, branch.getLeftKey());

                if (tier.isFull())
                {
                    if (listOfFullTiers.isEmpty())
                    {
                        listOfFullTiers.add(grandParent);
                    }
                    listOfFullTiers.add(parent);
                }
                else
                {
                    listOfFullTiers.clear();
                }

                if (parent.getChildType() == LEAF)
                {
                    if (tier.isFull())
                    {
                        listOfFullTiers.add(tier);

                        Iterator ancestors = listOfFullTiers.iterator();
                        parent = (InnerTier) ancestors.next();
                        tier = (Tier) ancestors.next();
                        do
                        {
                            grandParent = parent;
                            parent = (InnerTier) tier;
                            Tier full = (Tier) ancestors.next();
                            Split split = full.split(txn, fields, keyOfObject, setOfDirty);
                            if (split == null)
                            {
                                tier = full;
                            }
                            else
                            {
                                if (parent == null)
                                {
                                    parent = (InnerTier) full;
                                    parent.splitRootTier(txn, split, setOfDirty);
                                }
                                else
                                {
                                    parent.replace(full, split, setOfDirty);
                                    if (grandParent != null)
                                    {
                                        grandParent.find(txn, fields).setSize(parent.getSize());
                                        setOfDirty.add(grandParent);
                                    }
                                }
                                branch = parent.find(txn, fields);
                                tier = parent.getTier(txn, branch.getLeftKey());
                            }
                        }
                        while (ancestors.hasNext());
                    }

                    LeafTier leaf = (LeafTier) tier;
                    branch.setSize(branch.getSize() + 1);
                    setOfDirty.add(parent);
                    leaf.insert(txn, fields, keyOfObject, setOfDirty);
                    break;
                }

                inner = (InnerTier) tier;
            }

            // FIXME Syncrhonize.
            size++;
        }

        public Cursor find(Object keyOfObject)
        {
            return find(structure.getFieldExtractor().getFields(txn, keyOfObject));
        }

        public Cursor find(Comparable[] fields)
        {
            InnerTier tier = getRoot();
            for (;;)
            {
                Branch branch = tier.find(txn, fields);
                if (tier.getChildType() == LEAF)
                {
                    LeafTier leaf = structure.getStorage().getLeafTier(structure, txn, branch.getLeftKey());
                    return leaf.find(txn, fields);
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
        }

        public Object remove(Object keyOfObject)
        {
            Comparable[] fields = structure.getFieldExtractor().getFields(txn, keyOfObject);
            TierSet setOfDirty = new TierSet(mapOfDirtyTiers);
            LinkedList listOfAncestors = new LinkedList();
            InnerTier inner = null;
            InnerTier parent = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                listOfAncestors.addLast(tier);
                parent = tier;
                Branch branch = parent.find(txn, fields);
                if (branch.getObject() != null && keyOfObject.equals(structure.getObjectKey(branch.getObject())))
                {
                    inner = tier;
                }
                if (tier.getChildType() == LEAF)
                {
                    LeafTier leaf = structure.getStorage().getLeafTier(structure, txn, branch.getLeftKey());
                    Collection collection = leaf.remove(txn, keyOfObject);
                    branch.setSize(leaf.getSize());
                    setOfDirty.add(parent);

                    if (inner != null)
                    {
                        int objectCount = leaf.getSize();
                        if (objectCount == 0)
                        {
                            int index = parent.getIndexOfTier(leaf);
                            if (inner.getChildType() == LEAF)
                            {
                                parent.removeLeafTier(leaf, setOfDirty);
                            }
                            else
                            {
                                Branch newPivot = parent.get(index - 1);
                                parent.removeLeafTier(leaf, setOfDirty);
                                parent.replacePivot(txn, structure.getFields(txn, newPivot.getObject()), null, setOfDirty);
                                inner.replacePivot(txn, fields, newPivot.getObject(), setOfDirty);
                            }
                        }
                        else
                        {
                            Object newPivot = leaf.get(objectCount - 1);
                            inner.replacePivot(txn, fields, newPivot, setOfDirty);
                        }
                    }

                    Tier child = leaf;
                    Iterator ancestors = listOfAncestors.iterator();
                    while (ancestors.hasNext())
                    {
                        InnerTier merge = (InnerTier) ancestors.next();
                        merge.merge(txn, child, setOfDirty);
                    }

                    size -= collection.size();
                    return collection;
                }
                tier = (InnerTier) parent.getTier(txn, branch.getLeftKey());
            }
        }

        public Cursor first()
        {
            Branch branch = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                branch = tier.get(0);
                if (tier.getChildType() == LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
            return new Cursor(structure, txn, (LeafTier) tier.getTier(txn, branch.getLeftKey()), 0);
        }

        public Cursor last()
        {
            Branch branch = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                branch = tier.get(tier.getSize());
                if (tier.getChildType() == LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
            LeafTier leaf = structure.getStorage().getLeafTier(structure, txn, branch.getLeftKey());
            return new Cursor(structure, txn, leaf, leaf.getSize());
        }

        public void write()
        {
            if (mapOfDirtyTiers.size() != 0)
            {
                Iterator tiers = mapOfDirtyTiers.values().iterator();
                while (tiers.hasNext())
                {
                    Tier tier = (Tier) tiers.next();
                    tier.write(txn);
                }
                mapOfDirtyTiers.clear();
            }
        }

        public void copacetic()
        {
            Comparator comparator = new CopaceticComparator(txn, structure);
            getRoot().copacetic(txn, new Copacetic(comparator));
            Strata.Cursor cursor = first();
            if (getSize() != 0)
            {
                Object previous = cursor.next();
                for (int i = 1; i < size; i++)
                {
                    if (!cursor.hasNext())
                    {
                        throw new IllegalStateException();
                    }
                    Object next = cursor.next();
                    if (comparator.compare(previous, next) > 0)
                    {
                        throw new IllegalStateException();
                    }
                    previous = next;
                }
                if (cursor.hasNext())
                {
                    throw new IllegalStateException();
                }
            }
            else if (cursor.hasNext())
            {
                throw new IllegalStateException();
            }
        }

        private InnerTier getRoot()
        {
            return structure.getStorage().getInnerTier(structure, txn, rootKey);
        }
    }

    public final static class Cursor
    {
        private final Structure structure;

        private final Object txn;

        private int index;

        private LeafTier leaf;

        public Cursor(Structure structure, Object txn, LeafTier leaf, int index)
        {
            this.structure = structure;
            this.txn = txn;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isForward()
        {
            return true;
        }

        public Cursor newCursor()
        {
            return new Cursor(structure, txn, leaf, index);
        }

        public boolean hasNext()
        {
            return index < leaf.getSize() || !structure.getStorage().isKeyNull(leaf.getNextLeafKey());
        }

        public Object next()
        {
            if (index == leaf.getSize())
            {
                Storage storage = structure.getStorage();
                if (storage.isKeyNull(leaf.getNextLeafKey()))
                {
                    throw new IllegalStateException();
                }
                leaf = structure.getStorage().getLeafTier(structure, txn, leaf.getNextLeafKey());
                index = 0;
            }
            return structure.getObjectKey(leaf.get(index++));
        }

        public boolean hasPrevious()
        {
            return index > 0 || !structure.getStorage().isKeyNull(leaf.getPreviousLeafKey());
        }

        public Object previous()
        {
            if (index == 0)
            {
                Storage storage = structure.getStorage();
                if (storage.isKeyNull(leaf.getPreviousLeafKey()))
                {
                    throw new IllegalStateException();
                }
                leaf = structure.getStorage().getLeafTier(structure, txn, leaf.getPreviousLeafKey());
                index = leaf.getSize();
            }
            return leaf.get(--index);
        }
    }

    public static class Copacetic
    {
        private final Set seen;

        private Copacetic(Comparator comparator)
        {
            this.seen = new TreeSet(comparator);
        }

        public boolean unique(Object object)
        {
            if (seen.contains(object))
            {
                return false;
            }
            seen.add(object);
            return true;
        }
    }

    private final static int compare(Comparable[] left, Comparable[] right)
    {
        if (left == null && right == null)
        {
            throw new IllegalStateException();
        }

        if (left == null)
        {
            return 1;
        }

        if (right == null)
        {
            return -1;
        }

        int count = Math.min(left.length, right.length);
        for (int i = 0; i < count; i++)
        {
            int compare = left[i].compareTo(right[i]);
            if (compare != 0)
            {
                return compare;
            }
        }

        return left.length - right.length;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */