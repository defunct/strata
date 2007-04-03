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

    public final static Cursor EMPTY_CURSOR = new EmptyCursor(true);

    private final Structure structure;

    private final Object rootKey;

    private int size;

    public Strata()
    {
        this(new ArrayListStorage(), null, new BasicCriteriaServer(), 5);
    }

    public Strata(Storage storage, Object txn, CriteriaServer criterion, int size)
    {
        Structure structure = new Structure(storage, criterion, size);
        InnerTier root = structure.getStorage().newInnerTier(structure, txn, Strata.LEAF);
        LeafTier leaf = structure.getStorage().newLeafTier(structure, txn);
        leaf.write(txn);

        root.add(new Branch(leaf.getKey(), Strata.TERMINAL, 0));
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
        private CriteriaServer criterion = new BasicCriteriaServer();

        private Storage storage = new ArrayListStorage();

        private int size = 5;

        public Strata create(Object txn)
        {
            return new Strata(storage, txn, criterion, size);
        }

        public void setStorage(Storage storage)
        {
            this.storage = storage;
        }

        public void setCriteriaServer(CriteriaServer criterion)
        {
            this.criterion = criterion;
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

        private final CriteriaServer criterion;

        private final int size;

        public Structure(Storage storage, CriteriaServer criterion, int size)
        {
            this.storage = storage;
            this.criterion = criterion;
            this.size = size;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public CriteriaServer getCriterion()
        {
            return criterion;
        }

        public int getSize()
        {
            return size;
        }

        public Comparator newComparator(Object txn)
        {
            return new CopaceticComparator(criterion, txn);
        }
    }

    private final static class CopaceticComparator
    implements Comparator
    {
        private final CriteriaServer criterion;

        private final Object txn;

        public CopaceticComparator(CriteriaServer criterion, Object txn)
        {
            this.criterion = criterion;
            this.txn = txn;
        }

        public int compare(Object left, Object right)
        {
            return criterion.newCriteria(txn, left).partialMatch(right);
        }
    }

    public interface Resolver
    {
        public Object resolve(Object txn, Object object);
    }

    public final static class BasicResolver
    implements Resolver, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Object resolve(Object txn, Object object)
        {
            return object;
        }
    }

    public interface Criteria
    {
        public Object getObject();

        public int partialMatch(Object object);

        public boolean exactMatch(Object object);
    }

    public interface CriteriaServer
    {
        public Criteria newCriteria(Object txn, Object object);

        public Criteria newCriteria(Comparison comparison, Object txn, Object object);
    }

    public interface Comparison
    {
        public int partialMatch(Object criteria, Object stored);

        // FIXME This is not actually used in a query, only in insert and
        // delete.
        // FIXME getObject is only used in insert.
        // FIXME Does insert use exactMatch?
        public boolean exactMatch(Object criteria, Object stored);
    }

    public static class ComplexCriteria
    implements Criteria, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Resolver resolver;

        private final Comparison comparison;

        private final Object txn;

        private final Object object;

        private final Object criteria;

        public ComplexCriteria(Resolver resolver, Comparison comparison, Object txn, Object object)
        {
            this.resolver = resolver;
            this.comparison = comparison;
            this.txn = txn;
            this.object = object;
            this.criteria = resolver.resolve(txn, object);
        }

        public int partialMatch(Object object)
        {
            return comparison.partialMatch(criteria, resolver.resolve(txn, object));
        }

        public boolean exactMatch(Object object)
        {
            return comparison.exactMatch(criteria, resolver.resolve(txn, object));
        }

        public Object getObject()
        {
            return object; // FIXME Broken. Why is it not used?
        }
    }

    public static class BasicComparison
    implements Comparison, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        protected int partialMatch(Comparable[] left, Comparable[] right)
        {
            for (int i = 0; i < left.length && i < right.length; i++)
            {
                int compare = left[i].compareTo(right[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return left.length - right.length;
        }

        public int partialMatch(Object criteria, Object stored)
        {
            return ((Comparable) criteria).compareTo(stored);
        }

        public boolean exactMatch(Object criteria, Object stored)
        {
            return criteria.equals(stored);
        }
    };

    public interface FieldExtractor
    {
        public Object[] getFields(Object object);
    }

    public static class FieldComparison
    implements Comparison, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final FieldExtractor storedFields;

        private final FieldExtractor soughtFields;

        public FieldComparison(FieldExtractor soughtFields, FieldExtractor storedFields)
        {
            this.soughtFields = soughtFields;
            this.storedFields = storedFields;
        }

        public int partialMatch(Object criteria, Object stored)
        {
            return partialMatch(soughtFields.getFields(criteria), storedFields.getFields(stored));
        }

        public boolean exactMatch(Object criteria, Object stored)
        {
            return exactMatch(soughtFields.getFields(criteria), storedFields.getFields(stored));
        }

        protected int partialMatch(Object[] left, Object[] right)
        {
            for (int i = 0; i < left.length && i < right.length; i++)
            {
                int compare = ((Comparable) left[i]).compareTo(right[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return left.length - right.length;
        }

        protected boolean exactMatch(Object[] left, Object[] right)
        {
            for (int i = 0; i < left.length && i < right.length; i++)
            {
                if (left[i].equals(right[i]))
                {
                    return false;
                }
            }
            return left.length == right.length;
        }
    };

    public static class ComplexCriteriaServer
    implements CriteriaServer, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Resolver resolver;

        private final Comparison comparison;

        public ComplexCriteriaServer(Resolver resolver, Comparison comparison)
        {
            this.resolver = resolver;
            this.comparison = comparison;
        }

        public Criteria newCriteria(Object txn, Object object)
        {
            return new ComplexCriteria(resolver, comparison, txn, object);
        }

        public Criteria newCriteria(Comparison comparison, Object txn, Object object)
        {
            return new ComplexCriteria(resolver, comparison, txn, object);
        }
    }

    // FIXME When is this used and how? Becomming moot? New find abstracts,
    // hides enough.
    public static class BasicCriteria
    implements Criteria, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Comparable comparable;

        public BasicCriteria(Comparable comparable)
        {
            this.comparable = comparable;
        }

        public int partialMatch(Object object)
        {
            return comparable.compareTo(object);
        }

        public boolean exactMatch(Object object)
        {
            return comparable.equals(object);
        }

        public Object getObject()
        {
            return comparable;
        }
    }

    public static class BasicCriteriaServer
    implements CriteriaServer, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Criteria newCriteria(Object txn, Object object)
        {
            return new BasicCriteria((Comparable) object);
        }

        public Criteria newCriteria(Comparison comparison, Object txn, Object object)
        {
            return new ComplexCriteria(new Strata.BasicResolver(), comparison, null, object);
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

    /**
     * @author Alan Gutierrez
     */
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

        public Strata.Split split(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty);

        /**
         * Merge the contents of a tier to the left of this tier into this
         * tier.
         * 
         * @param txn
         *            The transaction of the query.
         * @param left
         *            The tier to the left of this tier.
         */
        public void consume(Object txn, Tier left, Strata.TierSet setOfDirty);

        public void revert(Object txn);

        public void write(Object txn);

        public void copacetic(Object txn, Strata.Copacetic copacetic);
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

    public final static class LeafTier
    implements Strata.Tier
    {
        private Object addressOfNext;

        private Object addressOfPrevious;

        private final Object storageData;

        private final List listOfObjects;

        protected final Strata.Structure structure;

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

        public boolean isFull()
        {
            return getSize() == structure.getSize();
        }

        public Strata.Split split(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
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

            Strata.Criteria candidate = structure.getCriterion().newCriteria(txn, get(middle));
            for (int i = 0; partition == -1 && i < middle; i++)
            {
                if (candidate.partialMatch(get(lesser)) != 0)
                {
                    partition = lesser + 1;
                }
                else if (candidate.partialMatch(get(greater)) != 0)
                {
                    partition = greater;
                }
                lesser--;
                greater++;
            }

            Strata.Storage storage = structure.getStorage();
            Strata.Split split = null;
            if (partition == -1)
            {
                Object repeated = get(0);
                int compare = criteria.partialMatch(repeated);
                if (compare < 0)
                {
                    LeafTier right = storage.newLeafTier(structure, txn);
                    while (getSize() != 0)
                    {
                        right.add(remove(0));
                    }

                    link(txn, this, right, setOfDirty);

                    split = new Strata.Split(criteria.getObject(), right);
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

                    split = new Strata.Split(repeated, right);
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

                split = new Strata.Split(get(getSize() - 1), right);
            }

            return split;
        }

        public void append(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
        {
            if (getSize() == structure.getSize())
            {
                ensureNextLeafTier(txn, criteria, setOfDirty);
                getNext(txn).append(txn, criteria, setOfDirty);
            }
            else
            {
                add(criteria.getObject());
                setOfDirty.add(this);
            }
        }

        public void insert(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
        {
            if (getSize() == structure.getSize())
            {
                ensureNextLeafTier(txn, criteria, setOfDirty);
                getNext(txn).append(txn, criteria, setOfDirty);
            }
            else
            {
                ListIterator objects = listIterator();
                while (objects.hasNext())
                {
                    Object before = objects.next();
                    if (criteria.partialMatch(before) <= 0)
                    {
                        objects.previous();
                        objects.add(criteria.getObject());
                        break;
                    }
                }

                if (!objects.hasNext())
                {
                    add(criteria.getObject());
                }

                setOfDirty.add(this);
            }
        }

        public Strata.Cursor find(Object txn, Strata.Criteria criteria)
        {
            for (int i = 0; i < getSize(); i++)
            {
                Object before = get(i);
                if (criteria.partialMatch(before) == 0)
                {
                    return new Strata.ForwardCursor(structure, txn, this, i);
                }
            }
            return Strata.EMPTY_CURSOR;
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
        public Collection remove(Object txn, Strata.Criteria criteria)
        {
            List listOfRemoved = new ArrayList();
            Iterator objects = listIterator();
            while (objects.hasNext())
            {
                Object candidate = objects.next();
                if (criteria.exactMatch(candidate))
                {
                    listOfRemoved.add(candidate);
                    objects.remove();
                }
            }
            Strata.Storage storage = structure.getStorage();
            LeafTier lastLeaf = this;
            LeafTier leaf = null;
            while (!storage.isKeyNull(lastLeaf.getNextLeafKey()) && criteria.exactMatch((leaf = lastLeaf.getNext(txn)).get(0)))
            {
                objects = leaf.listIterator();
                while (objects.hasNext())
                {
                    Object candidate = objects.next();
                    if (criteria.equals(candidate))
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

        public void consume(Object txn, Strata.Tier left, Strata.TierSet setOfDirty)
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

        public void revert(Object txn)
        {
            structure.getStorage().revert(structure, txn, this);
        }

        public void write(Object txn)
        {
            structure.getStorage().write(structure, txn, this);
        }

        public void copacetic(Object txn, Strata.Copacetic copacetic)
        {
            if (getSize() < 1)
            {
                throw new IllegalStateException();
            }
            Object previous = null;
            Iterator objects = listIterator();
            Comparator comparator = structure.newComparator(txn);
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

        private LeafTier getPrevious(Object txn)
        {
            return (LeafTier) structure.getStorage().getLeafTier(structure, txn, getPreviousLeafKey());
        }

        private LeafTier getNext(Object txn)
        {
            return (LeafTier) structure.getStorage().getLeafTier(structure, txn, getNextLeafKey());
        }

        private boolean endOfList(Object txn, Object object, LeafTier last)
        {
            return structure.getStorage().isKeyNull(last.getNextLeafKey()) || structure.newComparator(txn).compare(last.getNext(txn).get(0), object) != 0;
        }

        private void ensureNextLeafTier(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
        {
            Strata.Storage storage = structure.getStorage();
            if (storage.isKeyNull(getNextLeafKey()) || criteria.partialMatch(getNext(txn).get(0)) != 0)
            {
                LeafTier nextLeaf = storage.newLeafTier(structure, txn);
                link(txn, this, nextLeaf, setOfDirty);
            }
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

        public Object getStorageData()
        {
            return storageData;
        }

        public int getSize()
        {
            return listOfObjects.size();
        }

        public Object get(int index)
        {
            return listOfObjects.get(index);
        }

        public String toString()
        {
            return listOfObjects.toString();
        }

        public Object remove(int index)
        {
            return listOfObjects.remove(index);
        }

        public void add(Object object)
        {
            listOfObjects.add(object);
        }

        public void shift(Object object)
        {
            listOfObjects.add(0, object);
        }

        public ListIterator listIterator()
        {
            return listOfObjects.listIterator();
        }

        public Object getPreviousLeafKey()
        {
            return addressOfPrevious;
        }

        public void setPreviousLeafKey(Object previousLeafKey)
        {
            this.addressOfPrevious = previousLeafKey;
        }

        public Object getNextLeafKey()
        {
            return addressOfNext;
        }

        public void setNextLeafKey(Object nextLeafKey)
        {
            this.addressOfNext = nextLeafKey;
        }

    }

    public final static Object TERMINAL = new Object()
    {
        public String toString()
        {
            return "TERMINAL";
        }
    };

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
            return object == TERMINAL;
        }

        public String toString()
        {
            return object.toString();
        }
    }

    public final static class InnerTier
    implements Strata.Tier
    {
        protected final Strata.Structure structure;

        private final Object key;

        private short childType;

        private final List listOfBranches;

        public InnerTier(Strata.Structure structure, Object key)
        {
            this.structure = structure;
            this.key = key;
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

        public int getType()
        {
            return Strata.INNER;
        }

        public Branch get(int index)
        {
            return (Branch) listOfBranches.get(index);
        }

        public Strata.Split split(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
        {
            int partition = (structure.getSize() + 1) / 2;

            Strata.Storage storage = structure.getStorage();
            InnerTier right = storage.newInnerTier(structure, txn, getChildType());
            for (int i = partition; i < structure.getSize() + 1; i++)
            {
                right.add(remove(partition));
            }

            Branch branch = remove(getSize());
            Object pivot = branch.getObject();
            add(new Branch(branch.getLeftKey(), Strata.TERMINAL, branch.getSize()));

            setOfDirty.add(this);
            setOfDirty.add(right);

            return new Strata.Split(pivot, right);
        }

        public boolean isFull()
        {
            return getSize() == structure.getSize();
        }

        public Branch find(Strata.Criteria criteria)
        {
            Iterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.isTerminal() || criteria.partialMatch(branch.getObject()) <= 0)
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

        public Object removeLeafTier(Object keyOfLeafTier, Strata.TierSet setOfDirty)
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

        public void replacePivot(Strata.Criteria oldPivot, Object newPivot, Strata.TierSet setOfDirty)
        {
            ListIterator branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (oldPivot.partialMatch(branch.getObject()) == 0)
                {
                    branches.set(new Branch(branch.getLeftKey(), newPivot, branch.getSize()));
                    setOfDirty.add(this);
                    break;
                }
            }
        }

        public final void splitRootTier(Object txn, Strata.Split split, Strata.TierSet setOfDirty)
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
            add(new Branch(split.getRight().getKey(), Strata.TERMINAL, split.getRight().getSize()));
            setChildType(Strata.INNER);

            setOfDirty.add(this);
        }

        public void replace(Strata.Tier tier, Strata.Split split, Strata.TierSet setOfDirty)
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

        public boolean canMerge(Strata.Tier tier)
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

        public boolean merge(Object txn, Strata.Tier tier, Strata.TierSet setOfDirty)
        {
            int index = getIndexOfTier(tier.getKey());
            if (canMerge(index - 1, index))
            {
                merge(txn, index - 1, index, setOfDirty);
                return true;
            }
            else if (canMerge(index, index + 1))
            {
                merge(txn, index, index + 1, setOfDirty);
                return true;
            }
            return false;
        }

        public void consume(Object txn, Strata.Tier left, Strata.TierSet setOfDirty)
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

        public void revert(Object txn)
        {
            structure.getStorage().revert(structure, txn, this);
        }

        public void write(Object txn)
        {
            structure.getStorage().write(structure, txn, this);
        }

        public void copacetic(Object txn, Strata.Copacetic copacetic)
        {
            if (getSize() < 0)
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Object lastLeftmost = null;

            Comparator comparator = structure.newComparator(txn);
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

        private Object getLeftMost(Object txn, Strata.Tier tier, short childType)
        {
            while (childType != Strata.LEAF)
            {
                InnerTier inner = (InnerTier) tier;
                childType = inner.getChildType();
                tier = inner.getTier(txn, inner.get(0).getLeftKey());
            }
            LeafTier leaf = (LeafTier) tier;
            return leaf.get(0);
        }

        private boolean canMerge(int indexOfLeft, int indexOfRight)
        {
            return indexOfLeft >= 0 && indexOfRight <= getSize() && get(indexOfLeft).getSize() + get(indexOfRight).getSize() < structure.getSize() + 1;
        }

        public Strata.Tier getTier(Object txn, Object key)
        {
            if (getChildType() == Strata.INNER)
                return structure.getStorage().getInnerTier(structure, txn, key);
            return structure.getStorage().getLeafTier(structure, txn, key);
        }

        private void merge(Object txn, int indexOfLeft, int indexOfRight, Strata.TierSet setOfDirty)
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

        public LeafTier newLeafTier(Strata.Structure structure, Object txn);

        public LeafTier getLeafTier(Strata.Structure structure, Object txn, Object key);

        public InnerTier getInnerTier(Strata.Structure structure, Object txn, Object key);

        public void write(Strata.Structure structure, Object txn, InnerTier inner);

        public void write(Strata.Structure structure, Object txn, LeafTier leaf);

        public void free(Strata.Structure structure, Object txn, InnerTier inner);

        public void free(Strata.Structure structure, Object txn, LeafTier leaf);

        public void revert(Strata.Structure structure, Object txn, InnerTier inner);

        public void revert(Strata.Structure structure, Object txn, LeafTier leaf);

        public Object getKey(Strata.Tier leaf);

        public Object getNullKey();

        public boolean isKeyNull(Object object);
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

        public void insert(Object object)
        {
            Criteria criteria = structure.getCriterion().newCriteria(txn, object);

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
                Branch branch = parent.find(criteria);
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

                if (parent.getChildType() == Strata.LEAF)
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
                            Split split = full.split(txn, criteria, setOfDirty);
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
                                        grandParent.find(criteria).setSize(parent.getSize());
                                        setOfDirty.add(grandParent);
                                    }
                                }
                                branch = parent.find(criteria);
                                tier = parent.getTier(txn, branch.getLeftKey());
                            }
                        }
                        while (ancestors.hasNext());
                    }

                    LeafTier leaf = (LeafTier) tier;
                    branch.setSize(branch.getSize() + 1);
                    setOfDirty.add(parent);
                    leaf.insert(txn, criteria, setOfDirty);
                    break;
                }

                inner = (InnerTier) tier;
            }

            size++;
        }

        public Cursor find(Object object)
        {
            return find(structure.getCriterion().newCriteria(txn, object));
        }

        public Cursor find(Comparison comparison, Object object)
        {
            return find(structure.getCriterion().newCriteria(comparison, txn, object));
        }

        public Cursor find(Strata.Criteria criteria)
        {
            InnerTier tier = getRoot();
            for (;;)
            {
                Branch branch = tier.find(criteria);
                if (tier.getChildType() == Strata.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.getTier(txn, branch.getLeftKey());
                    return leaf.find(txn, criteria);
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
        }

        public Collection remove(Object object)
        {
            Criteria criteria = structure.getCriterion().newCriteria(txn, object);
            TierSet setOfDirty = new TierSet(mapOfDirtyTiers);
            LinkedList listOfAncestors = new LinkedList();
            InnerTier inner = null;
            InnerTier parent = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                listOfAncestors.addLast(tier);
                parent = tier;
                Branch branch = parent.find(criteria);
                if (branch.getObject() != Strata.TERMINAL && criteria.exactMatch(branch.getObject()))
                {
                    inner = tier;
                }
                if (tier.getChildType() == Strata.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.getTier(txn, branch.getLeftKey());
                    Collection collection = leaf.remove(txn, criteria);
                    branch.setSize(leaf.getSize());
                    setOfDirty.add(parent);

                    if (inner != null)
                    {
                        int objectCount = leaf.getSize();
                        if (objectCount == 0)
                        {
                            int index = parent.getIndexOfTier(leaf);
                            if (inner.getChildType() == Strata.LEAF)
                            {
                                parent.removeLeafTier(leaf, setOfDirty);
                            }
                            else
                            {
                                Branch newPivot = parent.get(index - 1);
                                parent.removeLeafTier(leaf, setOfDirty);
                                parent.replacePivot(structure.getCriterion().newCriteria(txn, newPivot.getObject()), Strata.TERMINAL, setOfDirty);
                                inner.replacePivot(criteria, newPivot.getObject(), setOfDirty);
                            }
                        }
                        else
                        {
                            Object newPivot = leaf.get(objectCount - 1);
                            inner.replacePivot(criteria, newPivot, setOfDirty);
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

        public Cursor values()
        {
            Branch branch = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                branch = tier.get(0);
                if (tier.getChildType() == Strata.LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
            return new ForwardCursor(structure, txn, (LeafTier) tier.getTier(txn, branch.getLeftKey()), 0);
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

        public void revert()
        {
            if (mapOfDirtyTiers.size() != 0)
            {
                Iterator tiers = mapOfDirtyTiers.values().iterator();
                while (tiers.hasNext())
                {
                    Tier tier = (Tier) tiers.next();
                    tier.revert(txn);
                }
                mapOfDirtyTiers.clear();
            }
        }

        public void copacetic()
        {
            getRoot().copacetic(txn, new Copacetic(structure.newComparator(txn)));
            Iterator iterator = values();
            if (getSize() != 0)
            {
                Comparator comparator = structure.newComparator(txn);
                Object previous = iterator.next();
                for (int i = 1; i < size; i++)
                {
                    if (!iterator.hasNext())
                    {
                        throw new IllegalStateException();
                    }
                    Object next = iterator.next();
                    if (comparator.compare(previous, next) > 0)
                    {
                        throw new IllegalStateException();
                    }
                    previous = next;
                }
                if (iterator.hasNext())
                {
                    throw new IllegalStateException();
                }
            }
            else if (iterator.hasNext())
            {
                throw new IllegalStateException();
            }
        }

        private InnerTier getRoot()
        {
            return structure.getStorage().getInnerTier(structure, txn, rootKey);
        }
    }

    public interface Cursor
    extends Iterator
    {
        public boolean isEmpty();

        public boolean isForward();

        public Cursor reverse();

        public Cursor newCursor();
    }

    public final static class ForwardCursor
    implements Cursor
    {
        private final Strata.Structure structure;

        private final Object txn;

        private int index;

        private LeafTier leaf;

        public ForwardCursor(Strata.Structure structure, Object txn, LeafTier leaf, int index)
        {
            this.structure = structure;
            this.txn = txn;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public boolean isForward()
        {
            return true;
        }

        public Cursor newCursor()
        {
            return new ForwardCursor(structure, txn, leaf, index);
        }

        public Cursor reverse()
        {
            return new ReverseCursor(structure, txn, leaf, index);
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
            return leaf.get(index++);
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public final static class ReverseCursor
    implements Cursor
    {
        private final Strata.Structure structure;

        private final Object txn;

        private int index;

        private LeafTier leaf;

        public ReverseCursor(Strata.Structure structure, Object txn, LeafTier leaf, int index)
        {
            this.structure = structure;
            this.txn = txn;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public boolean isForward()
        {
            return false;
        }

        public Cursor newCursor()
        {
            return new ReverseCursor(structure, txn, leaf, index);
        }

        public Cursor reverse()
        {
            return new ForwardCursor(structure, txn, leaf, index);
        }

        public boolean hasNext()
        {
            return index > 0 || !structure.getStorage().isKeyNull(leaf.getPreviousLeafKey());
        }

        public Object next()
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

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private final static class EmptyCursor
    implements Cursor
    {
        public final boolean forward;

        public EmptyCursor(boolean forward)
        {
            this.forward = forward;
        }

        public boolean isEmpty()
        {
            return true;
        }

        public boolean isForward()
        {
            return forward;
        }

        public Cursor newCursor()
        {
            return new EmptyCursor(forward);
        }

        public Cursor reverse()
        {
            return new EmptyCursor(!forward);
        }

        public boolean hasNext()
        {
            return false;
        }

        public Object next()
        {
            return new IllegalStateException();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
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
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */