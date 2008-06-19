package com.agtrz.strata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Strata
implements Serializable
{
    private static final long serialVersionUID = 20070207L;

    public final static short INNER = 1;

    public final static short LEAF = 2;

    private final static Object RESULT = new Object();

    private final static Object REPLACEMENT = new Object();

    private final static Object DELETING = new Object();

    private final static Object LEFT_LEAF = new Object();

    private final static Object SEARCH = new Object();

    private final Structure structure;

    private final Object rootKey;

    private transient Lock writeMutex;

    private Strata(Schema creator, Object txn, Map<Object, Tier> mapOfDirtyTiers)
    {
        Storage storage = creator.getStorageSchema().newStorage();

        this.structure = new Structure(creator, storage);
        // FIXME Shouldn't this be zero?
//        this.writeMutex = creator.getMaxDirtyTiers() == 1 ? (Lock) new NullSync() : (Lock) new ReentrantLock();
        this.writeMutex = new ReentrantLock();
        this.writeMutex.lock();

        InnerTier root = storage.newInnerTier(structure, txn, LEAF);
        LeafTier leaf = storage.newLeafTier(structure, txn);
        root.add(new Branch(leaf.getKey(), null));

        mapOfDirtyTiers.put(root.getKey(), root);
        mapOfDirtyTiers.put(leaf.getKey(), leaf);

        this.rootKey = root.getKey();
    }

    public Schema getSchema()
    {
        return new Schema(structure.getSchema());
    }

    public Query query(Object txn)
    {
        return new Query(txn, this, new HashMap<Object, Tier>());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
//        writeMutex = structure.getSchema().getMaxDirtyTiers() == 1 ? (Sync) new NullSync() : (Sync) new Mutex();
        writeMutex = new ReentrantLock();
    }
    
    @SuppressWarnings("unchecked")
    private final static int compare(Comparable<?> left, Comparable<?> right)
    {
        return ((Comparable) left).compareTo(right);
    }

    private final static int compare(Comparable<?>[] left, Comparable<?>[] right)
    {
        if (left == null)
        {
            if (right == null)
            {
                throw new IllegalStateException();
            }
            return -1;
        }
        else if (right == null)
        {
            return 1;
        }

        int count = Math.min(left.length, right.length);
        for (int i = 0; i < count; i++)
        {
            if (left[i] == null)
            {
                if (right[i] != null)
                {
                    return -1;
                }
            }
            else if (right[i] == null)
            {
                return 1;
            }
            else
            {
                int compare = compare(left[i], right[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
        }

        return left.length - right.length;
    }

    public final static class Danger
    extends RuntimeException
    {
        private static final long serialVersionUID = 20070513L;

        public Danger(Throwable cause, String message)
        {
            super(message, cause);
        }
    }
    
    public final static class Schema
    implements Serializable
    {
        private final static long serialVersionUID = 20070402L;

        private Storage.Schema storageSchema;

        private FieldExtractor extractor;

        private int leafSize;
        
        private int innerSize;

        private boolean cacheFields;

        private int maxDirtyTiers;

        public Schema()
        {
            this.storageSchema = new ArrayListStorage.Schema();
            this.extractor = new ComparableExtractor();
            this.leafSize = 5;
            this.innerSize = 5;
            this.cacheFields = false;
            this.maxDirtyTiers = 0;
        }

        public Schema(Schema creator)
        {
            this.storageSchema = creator.storageSchema;
            this.extractor = creator.extractor;
            this.leafSize = creator.leafSize;
            this.innerSize = creator.innerSize;
            this.cacheFields = creator.cacheFields;
            this.maxDirtyTiers = creator.maxDirtyTiers;
        }

        public Strata newStrata(Object txn)
        {
            Query query = newQuery(txn);
            query.flush();
            return query.getStrata();
        }

        public Query newQuery(Object txn)
        {
            Map<Object, Tier> mapOfDirtyTiers = new HashMap<Object, Tier>();
            Strata strata = new Strata(this, txn, mapOfDirtyTiers);
            Query query = new Query(txn, strata, mapOfDirtyTiers);
            if (mapOfDirtyTiers.size() >= getMaxDirtyTiers())
            {
                query.flush();
            }
            return query;
        }

        public void setStorage(Storage.Schema storageSchema)
        {
            this.storageSchema = storageSchema;
        }

        public Storage.Schema getStorageSchema()
        {
            return storageSchema;
        }

        public void setFieldExtractor(FieldExtractor extractor)
        {
            this.extractor = extractor;
        }

        public FieldExtractor getFieldExtractor()
        {
            return extractor;
        }

        public void setCacheFields(boolean cacheFields)
        {
            this.cacheFields = cacheFields;
        }

        public boolean getCacheFields()
        {
            return cacheFields;
        }

        public void setInnerSize(int innerSize)
        {
            this.innerSize = innerSize;
        }
        
        public void setLeafSize(int leafSize)
        {
            this.leafSize = leafSize;
        }

        public void setSize(int size)
        {
            setInnerSize(size);
            setLeafSize(size);
        }
        
        public int getInnerSize()
        {
            return innerSize;
        }

        public int getLeafSize()
        {
            return leafSize;
        }

        public void setMaxDirtyTiers(int maxDirtyTiers)
        {
            this.maxDirtyTiers = maxDirtyTiers;
        }

        public int getMaxDirtyTiers()
        {
            return maxDirtyTiers;
        }
    }

    public static class Structure
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Schema schema;

        private final Storage storage;

        private final Cooper cooper;

        public Structure(Schema schema, Storage storage)
        {
            this.schema = schema;
            this.storage = storage;
            this.cooper = schema.getCacheFields() ? (Cooper) new BucketCooper() : (Cooper) new LookupCooper();

        }

        public Schema getSchema()
        {
            return schema;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public Comparable<?>[] getFields(Object txn, Object object)
        {
            return cooper.getFields(txn, schema.getFieldExtractor(), object);
        }

        public Object newBucket(Comparable<?>[] fields, Object keyOfObject)
        {
            return cooper.newBucket(fields, keyOfObject);
        }

        public Object newBucket(Object txn, Object keyOfObject)
        {
            return cooper.newBucket(txn, schema.getFieldExtractor(), keyOfObject);
        }

        public Object getObjectKey(Object object)
        {
            return cooper.getObjectKey(object);
        }
    }

    public interface FieldExtractor
    {
        public Comparable<?>[] getFields(Object txn, Object object);
    }

    public final static class ComparableExtractor
    implements FieldExtractor, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        @SuppressWarnings("unchecked")
        private Comparable cast(Object object)
        {
            return (Comparable) object;
        }
        
        public Comparable<?>[] getFields(Object txn, Object object)
        {
            return new Comparable<?>[] { cast(object) };
        }
    }

    public final static class CopaceticComparator
    implements Comparator<Object>
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

    public interface Cooper
    {
        public Object newBucket(Object txn, FieldExtractor fields, Object keyOfObject);

        public Object newBucket(Comparable<?>[] fields, Object keyOfObject);

        public Object getObjectKey(Object object);

        public Comparable<?>[] getFields(Object txn, FieldExtractor extractor, Object object);

        public boolean getCacheFields();
    }

    private static final class Bucket
    {
        public final Comparable<?>[] fields;

        public final Object objectKey;

        public Bucket(Comparable<?>[] fields, Object objectKey)
        {
            this.fields = fields;
            this.objectKey = objectKey;
        }
    }

    public static class BucketCooper
    implements Cooper, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public Object newBucket(Object txn, FieldExtractor extractor, Object keyOfObject)
        {
            return new Bucket(extractor.getFields(txn, keyOfObject), keyOfObject);
        }

        public Object newBucket(Comparable<?>[] fields, Object keyOfObject)
        {
            return new Bucket(fields, keyOfObject);
        }

        public Comparable<?>[] getFields(Object txn, FieldExtractor extractor, Object object)
        {
            return ((Bucket) object).fields;
        }

        public Object getObjectKey(Object object)
        {
            return ((Bucket) object).objectKey;
        }

        public boolean getCacheFields()
        {
            return true;
        }
    }

    public final static class LookupCooper
    implements Cooper, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public Object newBucket(Object txn, FieldExtractor extractor, Object keyOfObject)
        {
            return keyOfObject;
        }

        public Object newBucket(Comparable<?>[] comparables, Object keyOfObject)
        {
            return keyOfObject;
        }

        public Comparable<?>[] getFields(Object txn, FieldExtractor extractor, Object object)
        {
            return extractor.getFields(txn, object);
        }

        public Object getObjectKey(Object object)
        {
            return object;
        }

        public boolean getCacheFields()
        {
            return false;
        }
    }

    public interface Tier
    {
        public ReadWriteLock getReadWriteLock();

        public Object getKey();

        public Object getStorageData();

        /**
         * Return the number of objects or objects used to pivot in this tier.
         * For an inner tier the size is the number of objects, while the
         * number of child tiers is the size plus one.
         * 
         * @return The size of the tier.
         */
        public int getSize();

        public void write(Object txn);

        public void copacetic(Object txn, Copacetic copacetic);
    }

    public final static class LeafTier
    implements Tier
    {
        private Object keyOfNext;

        private final Object storageData;

        private final List<Object> listOfObjects;

        private final Structure structure;

        private final ReadWriteLock readWriteLock;

        public LeafTier(Structure structure, Object storageData)
        {
            this.structure = structure;
            this.storageData = storageData;
            this.listOfObjects = new ArrayList<Object>(structure.getSchema().getLeafSize());
            // this.readWriteLock = new TracingReadWriteLock(new
            // ReentrantWriterPreferenceReadWriteLock(), getKey().toString() +
            // " " + System.currentTimeMillis());
            this.readWriteLock = new ReentrantReadWriteLock();
        }

        public ReadWriteLock getReadWriteLock()
        {
            return readWriteLock;
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

        public void setNextLeafKey(Object nextLeafKey)
        {
            this.keyOfNext = nextLeafKey;
        }

        public Object getNextLeafKey()
        {
            return keyOfNext;
        }

        public ListIterator<Object> listIterator()
        {
            return listOfObjects.listIterator();
        }

        public void add(Object txn, Object keyOfObject)
        {
            listOfObjects.add(structure.newBucket(txn, keyOfObject));
        }

        public void addBucket(Object bucket)
        {
            listOfObjects.add(bucket);
        }

        public Object remove(int index)
        {
            return listOfObjects.remove(index);
        }

        public Object get(int index)
        {
            return listOfObjects.get(index);
        }

        public LeafTier getNextAndLock(Mutation mutation, Level levelOfLeaf)
        {
            if (!structure.getStorage().isKeyNull(getNextLeafKey()))
            {
                LeafTier leaf = structure.getStorage().getLeafTier(structure, mutation.txn, getNextLeafKey());
                levelOfLeaf.lockAndAdd(leaf);
                return leaf;
            }
            return null;
        }

        private LeafTier getNext(Object txn)
        {
            return structure.getStorage().getLeafTier(structure, txn, getNextLeafKey());
        }

        public void link(LeafTier nextLeaf, Map<Object, Tier> mapOfDirtyTiers)
        {
            mapOfDirtyTiers.put(getKey(), this);
            mapOfDirtyTiers.put(nextLeaf.getKey(), nextLeaf);
            Object nextLeafKey = getNextLeafKey();
            setNextLeafKey(nextLeaf.getKey());
            nextLeaf.setNextLeafKey(nextLeafKey);
        }

        public void append(Mutation mutation, Level levelOfLeaf)
        {
            if (getSize() == structure.getSchema().getLeafSize())
            {
                LeafTier nextLeaf = getNextAndLock(mutation, levelOfLeaf);
                if (null == nextLeaf || compare(mutation.fields, mutation.getFields(nextLeaf.get(0))) != 0)
                {
                    nextLeaf = structure.getStorage().newLeafTier(structure, mutation.txn);
                    link(nextLeaf, mutation.mapOfDirtyTiers);
                }
                nextLeaf.append(mutation, levelOfLeaf);
            }
            else
            {
                addBucket(mutation.bucket);
                mutation.mapOfDirtyTiers.put(getKey(), this);
            }
        }

        public Cursor find(Object txn, Comparable<?>[] fields)
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

            if (getSize() > structure.getSchema().getLeafSize())
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Iterator<Object> objects = listIterator();
            Comparator<Object> comparator = new CopaceticComparator(txn, structure);
            while (objects.hasNext())
            {
                Object object = objects.next();
                if (previous != null && comparator.compare(previous, object) > 0)
                {
                    throw new IllegalStateException();
                }
                previous = object;
            }
            if (!structure.getStorage().isKeyNull(getNextLeafKey())
                && comparator.compare(get(getSize() - 1), getNext(txn).get(0)) == 0
                && structure.getSchema().getLeafSize() != getSize() && comparator.compare(get(0), get(getSize() - 1)) != 0)
            {
                throw new IllegalStateException();
            }
        }

    }

    public final static class Branch
    {
        private final Object rightKey;

        private Object pivot;

        public Branch(Object keyOfRight, Object pivot)
        {
            this.rightKey = keyOfRight;
            this.pivot = pivot;
        }

        public Object getRightKey()
        {
            return rightKey;
        }

        public Object getPivot()
        {
            return pivot;
        }

        public void setPivot(Object pivot)
        {
            this.pivot = pivot;
        }

        public boolean isMinimal()
        {
            return pivot == null;
        }

        public String toString()
        {
            return pivot == null ? "MINIMAL" : pivot.toString();
        }
    }

    public final static class InnerTier
    implements Tier
    {
        protected final Structure structure;

        private final Object key;

        private short childType;

        private final List<Branch> listOfBranches;

        private final ReadWriteLock readWriteLock;

        public InnerTier(Structure structure, Object key, short typeOfChildren)
        {
            this.structure = structure;
            this.key = key;
            this.childType = typeOfChildren;
            this.listOfBranches = new ArrayList<Branch>(structure.getSchema().getInnerSize());
            // this.readWriteLock = new TracingReadWriteLock(new
            // ReentrantWriterPreferenceReadWriteLock(), getKey().toString() +
            // " " + System.currentTimeMillis());
            this.readWriteLock = new ReentrantReadWriteLock();
        }

        public ReadWriteLock getReadWriteLock()
        {
            return readWriteLock;
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
            return listOfBranches.size();
        }

        public void setChildType(short childType)
        {
            this.childType = childType;
        }

        public void add(Object txn, Object keyOfLeft, Object keyOfObject)
        {
            Object bucket = null;
            if (keyOfObject != null)
            {
                bucket = structure.newBucket(txn, keyOfObject);
            }
            listOfBranches.add(new Branch(keyOfLeft, bucket));
        }

        public void add(Branch branch)
        {
            listOfBranches.add(branch);
        }

        public void add(int index, Branch branch)
        {
            listOfBranches.add(index, branch);
        }

        public Branch remove(int index)
        {
            return (Branch) listOfBranches.remove(index);
        }

        public ListIterator<Branch> listIterator()
        {
            return listOfBranches.listIterator();
        }

        public Branch get(int index)
        {
            return (Branch) listOfBranches.get(index);
        }

        public Branch find(Object txn, Comparable<?>[] fields)
        {
            Iterator<Branch> branches = listIterator();
            Branch candidate = branches.next();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (compare(fields, structure.getFields(txn, branch.getPivot())) < 0)
                {
                    break;
                }
                candidate = branch;
            }
            return candidate;
        }

        public int getIndexOfTier(Object keyOfTier)
        {
            int index = 0;
            Iterator<Branch> branches = listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.getRightKey().equals(keyOfTier))
                {
                    return index;
                }
                index++;
            }
            return -1;
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

            if (getSize() > structure.getSchema().getInnerSize())
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Object lastLeftmost = null;

            Comparator<Object> comparator = new CopaceticComparator(txn, structure);
            Iterator<Branch> branches = listIterator();
            boolean isMinimal = true;
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (isMinimal != branch.isMinimal())
                {
                    throw new IllegalStateException();
                }
                isMinimal = false;

                Tier left = getTier(txn, branch.getRightKey());
                left.copacetic(txn, copacetic);
                if (getChildType() == INNER && !(left instanceof InnerTier))
                {
                    throw new IllegalStateException();
                }
                if (getChildType() == LEAF && !(left instanceof LeafTier))
                {
                    throw new IllegalStateException();
                }

                if (!branch.isMinimal())
                {
                    // Each key must be less than the one next to it.
                    if (previous != null && comparator.compare(previous, branch.getPivot()) >= 0)
                    {
                        throw new IllegalStateException();
                    }

                    // Each key must occur only once in the inner tiers.
                    if (!copacetic.unique(branch.getPivot()))
                    {
                        throw new IllegalStateException();
                    }
                }
                previous = branch.getPivot();

                Object leftmost = getLeftMost(txn, left, getChildType());
                if (lastLeftmost != null && comparator.compare(lastLeftmost, leftmost) >= 0)
                {
                    throw new IllegalStateException();
                }
                if (previous != null && comparator.compare(previous, leftmost) != 0)
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
                tier = inner.getTier(txn, inner.get(0).getRightKey());
            }
            LeafTier leaf = (LeafTier) tier;
            return leaf.get(0);
        }

        public Tier getTier(Object txn, Object key)
        {
            if (getChildType() == INNER)
                return structure.getStorage().getInnerTier(structure, txn, key);
            return structure.getStorage().getLeafTier(structure, txn, key);
        }

        public String toString()
        {
            return listOfBranches.toString();
        }
    }

    public interface Storage
    extends Serializable
    {
        public InnerTier newInnerTier(Structure structure, Object txn, short typeOfChildren);

        public LeafTier newLeafTier(Structure structure, Object txn);

        public LeafTier getLeafTier(Structure structure, Object txn, Object key);

        public InnerTier getInnerTier(Structure structure, Object txn, Object key);

        public void write(Structure structure, Object txn, InnerTier inner);

        public void write(Structure structure, Object txn, LeafTier leaf);

        public void free(Structure structure, Object txn, InnerTier inner);

        public void free(Structure structure, Object txn, LeafTier leaf);

        public Object getKey(Tier tier);

        public Object getNullKey();

        public boolean isKeyNull(Object object);

        public void commit(Object txn);

        public Schema getSchema();

        public interface Schema
        {
            public Storage newStorage();
        }
    }

//    private final static class TracingSync
//    implements Sync
//    {
//        private final Sync sync;
//
//        private final String key;
//
//        public TracingSync(Sync sync, String key)
//        {
//            this.sync = sync;
//            this.key = key;
//        }
//
//        public void acquire() throws InterruptedException
//        {
//            System.out.println("Sync acquire (" + key + ")");
//            sync.acquire();
//        }
//
//        public boolean attempt(long timeout) throws InterruptedException
//        {
//            System.out.println("Sync attempt (" + key + ")");
//            return sync.attempt(timeout);
//        }
//
//        public void release()
//        {
//            System.out.println("Sync release (" + key + ")");
//            sync.release();
//        }
//    }
//
//    public final static class TracingReadWriteLock
//    implements ReadWriteLock
//    {
//        private final ReadWriteLock readWriteLock;
//
//        private final String key;
//
//        public TracingReadWriteLock(ReadWriteLock readWriteLock, String key)
//        {
//            this.readWriteLock = readWriteLock;
//            this.key = key;
//        }
//
//        public Sync readLock()
//        {
//            return new TracingSync(readWriteLock.readLock(), "read , " + key);
//        }
//
//        public Sync writeLock()
//        {
//            return new TracingSync(readWriteLock.writeLock(), "write, " + key);
//        }
//    }
//

    /**
     * A strategy for both caching dirty tiers in order to writing them out to
     * storage in a batch as well as for locking the Strata for exclusive
     * insert and delete.
     */
    interface TierCache
    {
        /**
         * Determines if the tier cache will invoke the commit method of the
         * storage implementation after the tier cache writes a set of dirty
         * tiers.
         *
         * @return True if the tier cache will auto commit.
         */
        public boolean isAutoCommit();
        
        /**
         * Sets whether the tier cache will invoke the commit method of the
         * storage implementation after the tier cache writes a set of dirty
         * tiers.
         *
         * @param autoCommit If true the tier cache will auto commit.
         */
        public void setAutoCommit(boolean autoCommit);
        
        /**
         * Lock the Strata exclusive for inserts and deletes. This does not
         * prevent other threads from reading the Strata.
         */
        public void lock();
        
        /**
         * A noop implementation of storage synchronization called before an
         * insert or delete of an object from the strata.
         */
        public void begin();
        
        /**
         * Record a tier as dirty in the tier cache.
         */
        public void dirty(Storage storage, Object txn, Tier tier);
        
        /**
         * A noop implementation of storage synchronization called after an
         * insert or delete of an object from the strata.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void end(Storage storage, Object txn);
        
        /**
         * Since the cache is always empty, this method merely calls the
         * commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void force(Storage storage, Object txn);
        
        /**
         * Lock the Strata for exclusive inserts and deletes. This does not
         * prevent other threads from reading the Strata.
         */
        public void unlock();
        
        /**
         * Create a new tier cache based on this prototype tier cache
         * instance. This is part of a prototype construction pattern.
         *
         * @return A new tier cache based on this prototype instance.
         */
        public TierCache newTierCache();
    }
    
    /**
     * A tier cache for in memory storage applications that merely implements
     * the ability to lock the common structure. This implementation
     * immediately calls the write method of the storage implementation when a
     * page is passed to the {@link NullTierCache#dirty dirty()} method. If
     * auto commit is true, the commit method of the storage strategy is
     * called immediately thereafter.
     * <p>
     * The auto commit property will retain the value set, but it does not
     * actually effect the behavior of storage.
     */
    public static class EmptyTierCache
    implements TierCache
    {
        /**
         * A lock instance that will exclusivly lock the Strata for insert and
         * delete. This lock instance is common to all tier caches generated
         * by the tier cache prototype.
         */
        protected final Lock lock;
        
        /**
         * A count of the number of times the lock method was called on this
         * tier cache instance.
         */
        protected int lockCount;
        
        /** 
         * If true the tier cache will invoke the commit method of the storage
         * implementation after the tier cache writes a set of dirty tiers.
         */
        private boolean autoCommit;

        /**
         * Create an empty tier cache with 
         */
        public EmptyTierCache(Lock lock)
        {
            this.lock = lock;
        }

        /**
         * Determines if the tier cache will invoke the commit method of the
         * storage implementation after the tier cache writes a set of dirty
         * tiers.
         *
         * @return True if the tier cache will auto commit.
         */
        public boolean isAutoCommit()
        {
            return autoCommit;
        }
        
        /**
         * Sets whether the tier cache will invoke the commit method of the
         * storage implementation after the tier cache writes a set of dirty
         * tiers.
         *
         * @param autoCommit If true the tier cache will auto commit.
         */
        public void setAutoCommit(boolean autoCommit)
        {
            this.autoCommit = autoCommit;
        }
        
        /**
         * Lock the Strata exclusive for iinserts and deletes. This does not
         * prevent other threads from reading the Strata.
         */
        public void lock()
        {
            if (lockCount == 0)
            {
                lock.lock();
            }
            lockCount++;
        }

        /**
         * A noop implementation of storage synchronization called before an
         * insert or delete of an object from the strata.
         */
        public void begin()
        {
        }

        /**
         * For the empty tier cache, this method immediately writes the dirty
         * tier to storage and commits the write if auto commit is enabled.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(Storage storage, Object txn, Tier tier)
        {
            tier.write(txn);
            if (isAutoCommit())
            {
                storage.commit(txn);
            }
        }
        
        /**
         * A noop implementation of storage synchronization called after an
         * insert or delete of an object from the strata.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void end(Storage storage, Object txn)
        {
        }
          
        /**
         * Since the cache is always empty, this method merely calls the
         * commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void force(Storage storage, Object txn)
        {
            storage.commit(txn);
        }
        
        /**
         * Lock the Strata for exclusive inserts and deletes. This does not
         * prevent other threads from reading the Strata.
         */
        public void unlock()
        {
            lockCount--;
            if (lockCount == 0)
            {
                lock.unlock();
            }
        }

        /**
         * Returns a new empty tier cache built from this prototype empty tier
         * cache. This will be a new empty tier cache that references the same
         * exclusive lock on the Strata.
         *
         * @return A new tier cache based on this prototype instance.
         */
        public TierCache newTierCache()
        {
            return new EmptyTierCache(lock);
        }
    }
    
    static class AbstractTierCache
    extends EmptyTierCache
    {
        protected final Map<Object, Tier> mapOfDirtyTiers;
        
        protected final int max;

        private boolean autoCommit;
        
        public AbstractTierCache(Lock lock, Map<Object, Tier> mapOfDirtyTiers, int max)
        {
            super(lock);
            this.mapOfDirtyTiers = mapOfDirtyTiers;
            this.max = max;
        }
        
        public boolean isAutoCommit()
        {
            return autoCommit;
        }
        
        public void setAutoCommit(boolean autoCommit)
        {
            this.autoCommit = autoCommit;
        }

        public void add(Tier tier)
        {
            synchronized (mapOfDirtyTiers)
            {
                mapOfDirtyTiers.put(tier.getKey(), tier);
            }
        }
        
        protected void save(Storage storage, Object txn, boolean force)
        {
            synchronized (mapOfDirtyTiers)
            {
                if (force || mapOfDirtyTiers.size() >= max)
                {
                    Iterator<Tier> tiers = mapOfDirtyTiers.values().iterator();
                    while (tiers.hasNext())
                    {
                        Tier tier = (Tier) tiers.next();
                        tier.write(txn);
                    }
                    mapOfDirtyTiers.clear();

                    if (force || isAutoCommit())
                    {
                        storage.commit(txn);
                    }
                }
            }
        }
        
        public void force(Storage storage, Object txn)
        {
            save(storage, txn, true);
        }
    }
     
    
    public static class PerQueryTierCache
    extends AbstractTierCache
    {
        private final Lock lock;
        
        public PerQueryTierCache(Lock lock, int max)
        {
            super(lock, new HashMap<Object, Tier>(), max);
            this.lock = lock;
        }
        
        public void begin()
        {
            if (mapOfDirtyTiers.size() == 0)
            {
                lock();
            }
        }
        
        public void end(Storage storage, Object txn)
        {
            save(storage, txn, false);
            if (mapOfDirtyTiers.size() == 0)
            {
                unlock();
            }
        }
        
        public TierCache newTierCache()
        {
            return new PerQueryTierCache(lock, max);
        }
    }
    
    public static class CommonTierCache
    extends AbstractTierCache
    {
        private final ReadWriteLock readWriteLock;
        public CommonTierCache(ReadWriteLock readWriteLock, int max)
        {
            super(readWriteLock.writeLock(), new HashMap<Object, Tier>(), max);
            this.readWriteLock = readWriteLock;
        }
        
        private CommonTierCache(ReadWriteLock readWriteLock, Map<Object, Tier> mapOfDirtyTiers, int max)
        {
            super(readWriteLock.writeLock(), mapOfDirtyTiers, max);
            this.readWriteLock = readWriteLock;
        }
        
        public void begin()
        {
            if (lockCount == 0)
            {
                readWriteLock.readLock().lock();
            }
        }
        
        public void end(Storage storage, Object txn)
        {
            save(storage, txn, false);
            if (lockCount == 0)
            {
                readWriteLock.readLock().unlock();
            }
        }
        
        public TierCache newTierCache()
        {
            return new CommonTierCache(readWriteLock, mapOfDirtyTiers, max);
        }
    }
    
    private final static class Mutation
    {
        public final Structure structure;

        public final Object txn;

        public final Comparable<?>[] fields;

        public final Deletable deletable;

        public final LinkedList<Level> listOfLevels = new LinkedList<Level>();

        public final Map<Object, Object> mapOfVariables = new HashMap<Object, Object>();

        public final Map<Object, Tier> mapOfDirtyTiers;

        public LeafOperation leafOperation;

        public final Object bucket;

        public Mutation(Structure structure, Object txn, Map<Object, Tier> mapOfDirtyTiers, Object bucket, Comparable<?>[] fields, Deletable deletable)
        {
            this.structure = structure;
            this.txn = txn;
            this.mapOfDirtyTiers = mapOfDirtyTiers;
            this.fields = fields;
            this.deletable = deletable;
            this.bucket = bucket;
        }

        public void rewind(int leaveExclusive)
        {
            Iterator<Level>levels = listOfLevels.iterator();
            int size = listOfLevels.size();
            boolean unlock = true;

            for (int i = 0; i < size - leaveExclusive; i++)
            {
                Level level = (Level) levels.next();
                Iterator<Operation> operations = level.listOfOperations.iterator();
                while (operations.hasNext())
                {
                    Operation operation = (Operation) operations.next();
                    if (operation.canCancel())
                    {
                        operations.remove();
                    }
                    else
                    {
                        unlock = false;
                    }
                }
                if (unlock)
                {
                    if (listOfLevels.size() == 3)
                    {
                        level.downgrade();
                    }
                    else
                    {
                        level.releaseAndClear();
                        levels.remove();
                    }
                }
            }
        }

        public void shift()
        {
            Iterator<Level> levels = listOfLevels.iterator();
            while (listOfLevels.size() > 3 && levels.hasNext())
            {
                Level level = levels.next();
                if (level.listOfOperations.size() != 0)
                {
                    break;
                }

                level.releaseAndClear();
                levels.remove();
            }
        }

        public Comparable<?>[] getFields(Object key)
        {
            return structure.getFields(txn, key);
        }
    }

    private interface LockExtractor
    {
        public Lock getSync(ReadWriteLock readWriteLock);

        public boolean isExeclusive();
    }

    private final static class ReadLockExtractor
    implements LockExtractor
    {
        public Lock getSync(ReadWriteLock readWriteLock)
        {
            return readWriteLock.readLock();
        }

        public boolean isExeclusive()
        {
            return false;
        }
    }

    private final static class WriteLockExtractor
    implements LockExtractor
    {
        public Lock getSync(ReadWriteLock readWriteLock)
        {
            return readWriteLock.writeLock();
        }

        public boolean isExeclusive()
        {
            return true;
        }
    }

    private interface LeafOperation
    {
        public boolean operate(Mutation mutation, Level levelOfLeaf);
    }

    private interface Operation
    {
        public void operate(Mutation mutation);

        public boolean canCancel();
    }

    private interface RootDecision
    {
        public boolean test(Mutation mutation, Level levelOfRoot, InnerTier root);

        public void operation(Mutation mutation, Level levelOfRoot, InnerTier root);
    }

    private interface Decision
    {
        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent);
    }

    public final static class SplitRoot
    implements RootDecision
    {
        public boolean test(Mutation mutation, Level levelOfRoot, InnerTier root)
        {
            return mutation.structure.getSchema().getInnerSize() == root.getSize();
        }

        public void operation(Mutation mutation, Level levelOfRoot, InnerTier root)
        {
            levelOfRoot.listOfOperations.add(new Split(root));
        }

        private final static class Split
        implements Operation
        {
            private final InnerTier root;

            public Split(InnerTier root)
            {
                this.root = root;
            }

            public void operate(Mutation mutation)
            {
                Storage storage = mutation.structure.getStorage();
                InnerTier left = storage.newInnerTier(mutation.structure, mutation.txn, root.getChildType());
                InnerTier right = storage.newInnerTier(mutation.structure, mutation.txn, root.getChildType());
                int partition = root.getSize() / 2;
                int fullSize = root.getSize();
                for (int i = 0; i < partition; i++)
                {
                    left.add(root.remove(0));
                }
                for (int i = partition; i < fullSize; i++)
                {
                    right.add(root.remove(0));
                }
                Object pivot = right.get(0).getPivot();
                right.get(0).setPivot(null);

                root.add(new Branch(left.getKey(), null));
                root.add(new Branch(right.getKey(), pivot));

                root.setChildType(INNER);

                mutation.mapOfDirtyTiers.put(root.getKey(), root);
                mutation.mapOfDirtyTiers.put(left.getKey(), left);
                mutation.mapOfDirtyTiers.put(right.getKey(), right);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class SplitInner
    implements Decision
    {
        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent)
        {
            Branch branch = parent.find(mutation.txn, mutation.fields);
            InnerTier child = (InnerTier) parent.getTier(mutation.txn, branch.getRightKey());
            levelOfChild.lockAndAdd(child);
            if (child.getSize() == mutation.structure.getSchema().getInnerSize())
            {
                levelOfParent.listOfOperations.add(new Split(parent, child));
                return true;
            }
            return false;
        }

        public final static class Split
        implements Operation
        {
            private final InnerTier parent;

            private final InnerTier child;

            public Split(InnerTier parent, InnerTier child)
            {
                this.parent = parent;
                this.child = child;
            }

            public void operate(Mutation mutation)
            {
                Storage storage = mutation.structure.getStorage();

                InnerTier right = storage.newInnerTier(mutation.structure, mutation.txn, child.getChildType());
                int partition = child.getSize() / 2;

                while (partition < child.getSize())
                {
                    right.add(child.remove(partition));
                }

                Object pivot = right.get(0).getPivot();
                right.get(0).setPivot(null);

                int index = parent.getIndexOfTier(child.getKey());
                parent.add(index + 1, new Branch(right.getKey(), pivot));

                mutation.mapOfDirtyTiers.put(parent.getKey(), parent);
                mutation.mapOfDirtyTiers.put(child.getKey(), child);
                mutation.mapOfDirtyTiers.put(right.getKey(), right);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class LeafInsert
    implements Decision
    {
        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent)
        {
            boolean split = true;
            levelOfChild.getSync = new WriteLockExtractor();
            Branch branch = parent.find(mutation.txn, mutation.fields);
            LeafTier leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
            levelOfChild.getSync = new WriteLockExtractor();
            levelOfChild.lockAndAdd(leaf);
            if (leaf.getSize() == mutation.structure.getSchema().getLeafSize())
            {
                Comparable<?>[] first = mutation.getFields(leaf.get(0));
                Comparable<?>[] last = mutation.getFields(leaf.get(leaf.getSize() - 1));
                if (compare(first, last) == 0)
                {
                    int compare = compare(mutation.fields, first);
                    if (compare < 0)
                    {
                        mutation.leafOperation = new SplitLinkedListLeft(parent);
                    }
                    else if (compare > 0)
                    {
                        mutation.leafOperation = new SplitLinkedListRight(parent);
                    }
                    else
                    {
                        mutation.leafOperation = new InsertLinkedList(leaf);
                        split = false;
                    }
                }
                else
                {
                    levelOfParent.listOfOperations.add(new SplitLeaf(parent));
                    mutation.leafOperation = new InsertSorted(parent);
                }
            }
            else
            {
                mutation.leafOperation = new InsertSorted(parent);
                split = false;
            }
            return split;
        }

        private final static class SplitLinkedListLeft
        implements LeafOperation
        {
            private final InnerTier inner;

            public SplitLinkedListLeft(InnerTier inner)
            {
                this.inner = inner;
            }

            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                Branch branch = inner.find(mutation.txn, mutation.fields);
                LeafTier leaf = (LeafTier) inner.getTier(mutation.txn, branch.getRightKey());

                LeafTier right = mutation.structure.getStorage().newLeafTier(mutation.structure, mutation.txn);
                while (leaf.getSize() != 0)
                {
                    right.addBucket(leaf.remove(0));
                }

                leaf.link(right, mutation.mapOfDirtyTiers);

                int index = inner.getIndexOfTier(leaf.getKey());
                if (index != 0)
                {
                    throw new IllegalStateException();
                }
                inner.add(index + 1, new Branch(right.getKey(), right.get(0)));

                mutation.mapOfDirtyTiers.put(inner.getKey(), inner);
                mutation.mapOfDirtyTiers.put(leaf.getKey(), leaf);
                mutation.mapOfDirtyTiers.put(right.getKey(), right);

                return new InsertSorted(inner).operate(mutation, levelOfLeaf);
            }
        }

        private final static class SplitLinkedListRight
        implements LeafOperation
        {
            private final InnerTier inner;

            public SplitLinkedListRight(InnerTier inner)
            {
                this.inner = inner;
            }

            private boolean endOfList(Mutation mutation, LeafTier last)
            {
                return mutation.structure.getStorage().isKeyNull(last.getNextLeafKey()) || compare(mutation.getFields(last.getNext(mutation.txn).get(0)), mutation.getFields(last.get(0))) != 0;
            }

            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                Branch branch = inner.find(mutation.txn, mutation.fields);
                LeafTier leaf = (LeafTier) inner.getTier(mutation.txn, branch.getRightKey());

                LeafTier last = leaf;
                while (!endOfList(mutation, last))
                {
                    last = last.getNextAndLock(mutation, levelOfLeaf);
                }

                LeafTier right = mutation.structure.getStorage().newLeafTier(mutation.structure, mutation.txn);
                last.link(right, mutation.mapOfDirtyTiers);

                inner.add(inner.getIndexOfTier(leaf.getKey()) + 1, new Branch(right.getKey(), mutation.bucket));

                mutation.mapOfDirtyTiers.put(inner.getKey(), inner);
                mutation.mapOfDirtyTiers.put(leaf.getKey(), leaf);
                mutation.mapOfDirtyTiers.put(right.getKey(), right);

                return new InsertSorted(inner).operate(mutation, levelOfLeaf);
            }
        }

        private final static class SplitLeaf
        implements Operation
        {
            private final InnerTier inner;

            public SplitLeaf(InnerTier inner)
            {
                this.inner = inner;
            }

            public void operate(Mutation mutation)
            {
                Branch branch = inner.find(mutation.txn, mutation.fields);
                LeafTier leaf = (LeafTier) inner.getTier(mutation.txn, branch.getRightKey());

                int middle = leaf.getSize() >> 1;
                boolean odd = (leaf.getSize() & 1) == 1;
                int lesser = middle - 1;
                int greater = odd ? middle + 1 : middle;

                int partition = -1;
                Comparable<?>[] candidate = mutation.getFields(leaf.get(middle));
                for (int i = 0; partition == -1 && i < middle; i++)
                {
                    if (compare(candidate, mutation.getFields(leaf.get(lesser))) != 0)
                    {
                        partition = lesser + 1;
                    }
                    else if (compare(candidate, mutation.getFields(leaf.get(greater))) != 0)
                    {
                        partition = greater;
                    }
                    lesser--;
                    greater++;
                }

                LeafTier right = mutation.structure.getStorage().newLeafTier(mutation.structure, mutation.txn);

                while (partition != leaf.getSize())
                {
                    right.addBucket(leaf.remove(partition));
                }

                leaf.link(right, mutation.mapOfDirtyTiers);

                int index = inner.getIndexOfTier(leaf.getKey());
                inner.add(index + 1, new Branch(right.getKey(), right.get(0)));

                mutation.mapOfDirtyTiers.put(inner.getKey(), inner);
                mutation.mapOfDirtyTiers.put(leaf.getKey(), leaf);
                mutation.mapOfDirtyTiers.put(right.getKey(), right);
            }

            public boolean canCancel()
            {
                return true;
            }
        }

        private final static class InsertSorted
        implements LeafOperation
        {
            private final InnerTier inner;

            public InsertSorted(InnerTier inner)
            {
                this.inner = inner;
            }

            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                Branch branch = inner.find(mutation.txn, mutation.fields);
                LeafTier leaf = (LeafTier) inner.getTier(mutation.txn, branch.getRightKey());

                Object bucket = mutation.bucket;

                ListIterator<Object> objects = leaf.listIterator();
                while (objects.hasNext())
                {
                    Object before = objects.next();
                    if (compare(mutation.getFields(before), mutation.fields) > 0)
                    {
                        objects.previous();
                        objects.add(bucket);
                        break;
                    }
                }

                if (!objects.hasNext())
                {
                    objects.add(bucket);
                }

                mutation.mapOfDirtyTiers.put(leaf.getKey(), leaf);

                return true;
            }
        }

        private final static class InsertLinkedList
        implements LeafOperation
        {
            private final LeafTier leaf;

            public InsertLinkedList(LeafTier leaf)
            {
                this.leaf = leaf;
            }

            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                leaf.append(mutation, levelOfLeaf);
                return true;
            }
        }
    }

    /**
     * Logic for deleting the 
     */
    private final static class DeleteRoot
    implements RootDecision
    {
        public boolean test(Mutation mutation, Level levelOfRoot, InnerTier root)
        {
            if (root.getChildType() == INNER && root.getSize() == 1)
            {
                InnerTier first = (InnerTier) root.getTier(mutation.txn, root.get(0).getRightKey());
                InnerTier second = (InnerTier) root.getTier(mutation.txn, root.get(1).getRightKey());
                return first.getSize() + second.getSize() == mutation.structure.getSchema().getInnerSize();
            }
            return false;
        }

        public void operation(Mutation mutation, Level levelOfRoot, InnerTier root)
        {
            levelOfRoot.listOfOperations.add(new Merge(root));
        }

        public final static class Merge
        implements Operation
        {
            private final InnerTier root;

            public Merge(InnerTier root)
            {
                this.root = root;
            }

            public void operate(Mutation mutation)
            {
                if (root.getSize() != 0)
                {
                    throw new IllegalStateException();
                }

                InnerTier child = (InnerTier) root.getTier(mutation.txn, root.remove(0).getRightKey());
                while (child.getSize() != 0)
                {
                    root.add(child.remove(0));
                }

                root.setChildType(child.getChildType());

                mutation.mapOfDirtyTiers.get(child.getKey());
                mutation.structure.getStorage().free(mutation.structure, mutation.txn, child);

                mutation.mapOfDirtyTiers.put(root.getKey(), root);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class InnerNever
    implements Decision
    {
        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent)
        {
            return false;
        }
    }

    private final static class SwapKey
    implements Decision
    {
        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent)
        {
            Branch branch = parent.find(mutation.txn, mutation.fields);
            if (branch.getPivot() != null && compare(mutation.getFields(branch.getPivot()), mutation.fields) == 0)
            {
                levelOfParent.listOfOperations.add(new Swap(parent));
                return true;
            }
            return false;
        }

        private final static class Swap
        implements Operation
        {
            private final InnerTier inner;

            public Swap(InnerTier inner)
            {
                this.inner = inner;
            }

            public void operate(Mutation mutation)
            {
                Object replacement = mutation.mapOfVariables.get(REPLACEMENT);
                if (replacement != null)
                {
                    Branch branch = inner.find(mutation.txn, mutation.fields);
                    branch.setPivot(replacement);
                    mutation.mapOfDirtyTiers.put(inner.getKey(), inner);
                }
            }

            public boolean canCancel()
            {
                return false;
            }
        }
    }

    /**
     * A decision that determines whether to merge two inner tiers into one
     * tier or else to delete an inner tier that has only one child tier but
     * is either the only child tier or its siblings are already full.
     * <h3>Only Children</h3>
     * <p>
     * It is possible that an inner tier may have only one child leaf or inner
     * tier. This occurs in the case where the siblings of of inner tier are
     * at capacity. A merge occurs when two children are combined. The nodes
     * from the child to the right are combined with the nodes from the child
     * to the left. The parent branch that referenced the right child is
     * deleted. 
     * <p>
     * If it is the case that a tier is next to full siblings, as leaves are
     * deleted from that tier, it will not be a candidate to merge with a
     * sibling until it reaches a size of one. At that point, it could merge
     * with a sibling if a deletion were to cause its size to reach zero.
     * <p>
     * However, the single child of that tier has no siblings with which it
     * can merge. A tier with a single child cannot reach a size of zero by
     * merging.
     * <p>
     * If were where to drain the subtree of an inner tier with a single child
     * of every leaf, we would merge its leaf tiers and merge its inner tiers
     * until we had subtree that consisted solely of inner tiers with one
     * child and a leaf with one item. At that point, when we delete the last
     * item, we need to delete the chain of tiers with only children.
     * <p>
     * We deleting any child that is size of one that cannot merge with a
     * sibling. Deletion means freeing the child and removing the branch that
     * references it.
     * <p>
     * The only leaf child will not have a sibling with which it can merge,
     * however. We won't be able to copy leaf items from a right leaf to a
     * left leaf. This means we won't be able to update the linked list of
     * leaves, unless we go to the left of the only child. But, going to the
     * left of the only child requires knowing that we must go to the left.
     * <p>
     * We are not going to know which left to take the first time down,
     * though. The actual pivot is not based on the number of children. It
     * might be above the point where the list of only children begins. As
     * always, it is a pivot whose value matches the first item in the
     * leaf, in this case the only item in the leaf.
     * <p>
     * Here's how it works.
     * <p>
     * On the way down, we look for a branch that has an inner tier that is
     * size of one. If so, we set a flag in the mutator to note that we are
     * now deleting.
     * <p>
     * If we encouter an inner tier has more than one child on the way down we
     * are not longer in the deleting state.
     * <p>
     * When we reach the leaf, if it has a size of one and we are in the
     * deleting state, then we look in the mutator for a left leaf variable
     * and an is left most flag. More on those later as neither are set.
     * <p>
     * We tell the mutator that we have a last item and that the action has
     * failed, by setting the fail action. Failure means we try again.
     * <p>
     * On the retry, as we desecend the tree, we have the last item variable
     * set in the mutator. 
     * <p>
     * Note that we are descending the tree again. Because we are a concurrent
     * data structure, the structure of the tree may change. I'll get to that.
     * For now, let's assume that it has not changed.
     * <p>
     * If it has not changed, then we are going to encouter a pivot that has
     * our last item. When we encouter this pivot, we are going to go left.
     * Going left means that we descend to the child of the branch before the
     * branch of the pivot. We then follow each rightmost branch of each inner
     * tier until we reach the right most leaf. That leaf is the leaf before
     * the leaf that is about to be removed. We store this in the mutator.
     * <p>
     * Of course, the leaf to be removed may be the left most leaf in the
     * entire data structure. In that case, we set a variable named left most
     * in the mutator.
     * <p>
     * When we go left, we lock every inner tier and the leaf tier exclusive,
     * to prevent it from being changed by another query in another thread.
     * We always lock from left to right.
     * <p>
     * Now we continue our desecnet. Eventually, we reach out chain of inner
     * tiers with only one child. That chain may only be one level deep, but
     * there will be such a chain.
     * <p>
     * Now we can add a remove leaf operation to the list of operations in the
     * parent level. This operation will link the next leaf of the left leaf
     * to the next leaf of the remove leaf, reserving our linked list of
     * leaves. It will take place after the normal remove operation, so that
     * if the remove operation fails (because the item to remove does not
     * actually exist) then the leave removal does not occur.
     * <p>
     * I revisted this logic after a year and it took me a while to convince
     * myself that it was not a misunderstanding on my earlier self's part,
     * that these linked lists of otherwise empty tiers are a natural
     * occurance.
     * <p>
     * The could be addressed by linking the inner tiers and thinking harder,
     * but that would increase the size of the project.
     */
    private final static class MergeInner
    implements Decision
    {
        /**
         * Determine if we are deleting a final leaf in a child and therefore
         * need to travel to the left.
         */
        private boolean lockLeft(Mutation mutation, Branch branch, Object search)
        {
            if (search != null && branch.getPivot() != null && !mutation.mapOfVariables.containsKey(LEFT_LEAF))
            {
                Comparable<?>[] searchFields = mutation.getFields(search);
                Comparable<?>[] pivotFields = mutation.getFields(branch.getPivot());
                return compare(searchFields, pivotFields) == 0;
            }
            return false;
        }

        public boolean test(Mutation mutation, Level levelOfParent,
                            Level levelOfChild, InnerTier parent)
        {
            Branch branch = parent.find(mutation.txn, mutation.fields);
            InnerTier child = (InnerTier) parent.getTier(mutation.txn, branch.getRightKey());

            Object search = mutation.mapOfVariables.get(SEARCH);

            if (lockLeft(mutation, branch, search))
            {
                // We have encountered a request to search for a replace
                Storage storage = mutation.structure.getStorage();
                int index = parent.getIndexOfTier(child.getKey()) - 1;
                InnerTier inner = parent;
                while (inner.getChildType() == INNER)
                {
                    inner = storage.getInnerTier(mutation.structure, mutation.txn, inner.get(index).getRightKey());
                    levelOfParent.lockAndAdd(inner);
                    index = inner.getSize() - 1;
                }
                LeafTier leaf = storage.getLeafTier(mutation.structure, mutation.txn, inner.get(index).getRightKey());
                levelOfParent.lockAndAdd(leaf);
                mutation.mapOfVariables.put(LEFT_LEAF, leaf);
            }

            if (child.getSize() == 1)
            {
                if (!mutation.mapOfVariables.containsKey(DELETING))
                {
                    mutation.mapOfVariables.put(DELETING, DELETING);
                }
                levelOfParent.listOfOperations.add(new Remove(parent, child));
                return true;
            }

            List<InnerTier> listToMerge = new ArrayList<InnerTier>();

            int index = parent.getIndexOfTier(child.getKey());
            if (index != 0)
            {
                InnerTier left = (InnerTier) parent.getTier(mutation.txn, parent.get(index - 1).getRightKey());
                levelOfChild.lockAndAdd(left);
                levelOfChild.lockAndAdd(child);
                if (left.getSize() + child.getSize() <= mutation.structure.getSchema().getInnerSize())
                {
                    listToMerge.add(left);
                    listToMerge.add(child);
                }
            }

            if (index == 0)
            {
                levelOfChild.lockAndAdd(child);
            }

            if (listToMerge.isEmpty() && index != parent.getSize() - 1)
            {
                InnerTier right = (InnerTier) parent.getTier(mutation.txn, parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(right);
                if ((child.getSize() + right.getSize() - 1) == mutation.structure.getSchema().getInnerSize())
                {
                    listToMerge.add(child);
                    listToMerge.add(right);
                }
            }

            if (listToMerge.size() != 0)
            {
                if (mutation.mapOfVariables.containsKey(DELETING))
                {
                    mutation.rewind(2);
                    mutation.mapOfVariables.remove(DELETING);
                }
                levelOfParent.listOfOperations.add(new Merge(parent, listToMerge));
                return true;
            }

            mutation.mapOfVariables.remove(DELETING);

            return false;
        }

        public final static class Merge
        implements Operation
        {
            private final InnerTier parent;

            private final List<InnerTier> listToMerge;

            public Merge(InnerTier parent, List<InnerTier> listToMerge)
            {
                this.parent = parent;
                this.listToMerge = listToMerge;
            }

            public void operate(Mutation mutation)
            {
                InnerTier left = (InnerTier) listToMerge.get(0);
                InnerTier right = (InnerTier) listToMerge.get(1);

                int index = parent.getIndexOfTier(right.getKey());
                Branch branch = parent.remove(index);

                right.get(0).setPivot(branch.getPivot());
                while (right.getSize() != 0)
                {
                    left.add(right.remove(0));
                }

                mutation.mapOfDirtyTiers.remove(right.getKey());
                mutation.structure.getStorage().free(mutation.structure, mutation.txn, right);

                mutation.mapOfDirtyTiers.put(parent.getKey(), parent);
                mutation.mapOfDirtyTiers.put(left.getKey(), left);
            }

            public boolean canCancel()
            {
                return true;
            }
        }

        public final static class Remove
        implements Operation
        {
            private final InnerTier parent;

            private final InnerTier child;

            public Remove(InnerTier parent, InnerTier child)
            {
                this.parent = parent;
                this.child = child;
            }

            public void operate(Mutation mutation)
            {
                int index = parent.getIndexOfTier(child.getKey());

                parent.remove(index);
                if (parent.getSize() != 0)
                {
                    parent.get(0).setPivot(null);
                }

                mutation.mapOfDirtyTiers.remove(child.getKey());
                mutation.structure.getStorage().free(mutation.structure, mutation.txn, child);

                mutation.mapOfDirtyTiers.put(parent.getKey(), parent);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class LeafRemove
    implements Decision
    {
        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent)
        {
            levelOfChild.getSync = new WriteLockExtractor();
            Branch branch = parent.find(mutation.txn, mutation.fields);
            int index = parent.getIndexOfTier(branch.getRightKey());
            LeafTier previous = null;
            LeafTier leaf = null;
            List<LeafTier> listToMerge = new ArrayList<LeafTier>();
            if (index != 0)
            {
                previous = (LeafTier) parent.getTier(mutation.txn, parent.get(index - 1).getRightKey());
                levelOfChild.lockAndAdd(previous);
                leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
                levelOfChild.lockAndAdd(leaf);
                int capacity = previous.getSize() + leaf.getSize();
                if (capacity <= mutation.structure.getSchema().getLeafSize() + 1)
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
                leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
                levelOfChild.lockAndAdd(leaf);
            }

            if (leaf.getSize() == 1 && parent.getSize() == 1 && mutation.mapOfVariables.containsKey(DELETING))
            {
                LeafTier left = (LeafTier) mutation.mapOfVariables.get(LEFT_LEAF);
                if (left == null)
                {
                    mutation.mapOfVariables.put(SEARCH, leaf.get(0));
                    mutation.leafOperation = new Fail();
                    return false;
                }

                levelOfParent.listOfOperations.add(new RemoveLeaf(parent, leaf, left));
                mutation.leafOperation = new Remove(leaf);
                return true;
            }
            else if (listToMerge.isEmpty() && index != parent.getSize() - 1)
            {
                LeafTier next = (LeafTier) parent.getTier(mutation.txn, parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(next);
                int capacity = next.getSize() + leaf.getSize();
                if (capacity <= mutation.structure.getSchema().getLeafSize() + 1)
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
                if (leaf == null)
                {
                    // FIXME This is dead code.
                    leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
                    levelOfChild.lockAndAdd(leaf);
                }
                mutation.leafOperation = new Remove(leaf);
            }
            else
            {
                // TODO Test that this activates.
                if (mutation.mapOfVariables.containsKey(DELETING))
                {
                    mutation.rewind(2);
                    mutation.mapOfVariables.remove(DELETING);
                }
                LeafTier left = (LeafTier) listToMerge.get(0);
                LeafTier right = (LeafTier) listToMerge.get(1);
                levelOfParent.listOfOperations.add(new Merge(parent, left, right));
                mutation.leafOperation = new Remove(leaf);
            }
            return !listToMerge.isEmpty();
        }

        public final static class Remove
        implements LeafOperation
        {
            private final LeafTier leaf;

            public Remove(LeafTier leaf)
            {
                this.leaf = leaf;
            }

            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                // TODO Remove single anywhere but far left.
                // TODO Remove single very left most.
                // TODO Remove single very right most.
                int count = 0;
                int found = 0;
                LeafTier current = leaf;
                SEARCH: do
                {
                    Iterator<Object> objects = leaf.listIterator();
                    while (objects.hasNext())
                    {
                        count++;
                        Object candidate = objects.next();
                        int compare = compare(mutation.fields, mutation.getFields(candidate));
                        if (compare < 0)
                        {
                            break SEARCH;
                        }
                        else if (compare == 0)
                        {
                            found++;
                            if (mutation.deletable.deletable(mutation.structure.getObjectKey(candidate)))
                            {
                                objects.remove();
                                if (count == 1)
                                {
                                    if (objects.hasNext())
                                    {
                                        mutation.mapOfVariables.put(REPLACEMENT, objects.next());
                                    }
                                    else
                                    {
                                        LeafTier following = current.getNextAndLock(mutation, levelOfLeaf);
                                        if (following != null)
                                        {
                                            mutation.mapOfVariables.put(REPLACEMENT, following.get(0));
                                        }
                                    }
                                }
                            }
                            mutation.mapOfDirtyTiers.put(current.getKey(), current);
                            mutation.mapOfVariables.put(RESULT, candidate);
                            break SEARCH;
                        }
                    }
                    current = current.getNextAndLock(mutation, levelOfLeaf);
                }
                while (current != null && compare(mutation.fields, mutation.getFields(current.get(0))) == 0);

                if (mutation.mapOfVariables.containsKey(RESULT) && count == found && current.getSize() == mutation.structure.getSchema().getLeafSize() - 1 && compare(mutation.fields, mutation.getFields(current.get(current.getSize() - 1))) == 0)
                {
                    for (;;)
                    {
                        LeafTier subsequent = current.getNextAndLock(mutation, levelOfLeaf);
                        if (subsequent == null || compare(mutation.fields, mutation.getFields(subsequent.get(0))) != 0)
                        {
                            break;
                        }
                        current.addBucket(subsequent.remove(0));
                        if (subsequent.getSize() == 0)
                        {
                            current.setNextLeafKey(subsequent.getNextLeafKey());
                            mutation.structure.getStorage().free(mutation.structure, mutation.txn, subsequent);
                        }
                        else
                        {
                            mutation.mapOfDirtyTiers.put(subsequent.getKey(), subsequent);
                        }
                        current = subsequent;
                    }
                }

                return mutation.mapOfVariables.containsKey(RESULT);
            }
        }

        public final class Fail
        implements LeafOperation
        {public Fail() { }
            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                return false;
            }
        }

        public final class Merge
        implements Operation
        {
            private final InnerTier parent;

            private final LeafTier left;

            private final LeafTier right;

            public Merge(InnerTier parent, LeafTier left, LeafTier right)
            {
                this.parent = parent;
                this.left = left;
                this.right = right;
            }

            public void operate(Mutation mutation)
            {
                parent.remove(parent.getIndexOfTier(right.getKey()));

                while (right.getSize() != 0)
                {
                    left.addBucket(right.remove(0));
                }
                // FIXME Get last leaf. 
                left.setNextLeafKey(right.getNextLeafKey());

                mutation.mapOfDirtyTiers.remove(right.getKey());
                mutation.structure.getStorage().free(mutation.structure, mutation.txn, right);

                mutation.mapOfDirtyTiers.put(parent.getKey(), parent);
                mutation.mapOfDirtyTiers.put(left.getKey(), left);
            }

            public boolean canCancel()
            {
                return true;
            }
        }

        public final static class RemoveLeaf
        implements Operation
        {
            private final InnerTier parent;

            private final LeafTier leaf;

            private final LeafTier left;

            public RemoveLeaf(InnerTier parent, LeafTier leaf, LeafTier left)
            {
                this.parent = parent;
                this.leaf = leaf;
                this.left = left;
            }

            public void operate(Mutation mutation)
            {
                parent.remove(parent.getIndexOfTier(leaf.getKey()));

                left.setNextLeafKey(leaf.getNextLeafKey());

                mutation.mapOfDirtyTiers.remove(leaf.getKey());
                mutation.structure.getStorage().free(mutation.structure, mutation.txn, leaf);

                mutation.mapOfDirtyTiers.put(parent.getKey(), parent);
                mutation.mapOfDirtyTiers.put(left.getKey(), left);

                mutation.mapOfVariables.remove(SEARCH);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class Level
    {
        public LockExtractor getSync;

        public final Map<Object, Tier> mapOfLockedTiers = new HashMap<Object, Tier>();

        public final LinkedList<Operation> listOfOperations = new LinkedList<Operation>();

        public Level(boolean exclusive)
        {
            this.getSync = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
        }

        public void lockAndAdd(Tier tier)
        {
            lock_(tier);
            add_(tier);
        }
        
        public void unlockAndRemove(Tier tier)
        {
            assert mapOfLockedTiers.containsKey(tier.getKey());
            
            mapOfLockedTiers.remove(tier.getKey());
            unlock_(tier);
        }

        public void add_(Tier tier)
        {
            mapOfLockedTiers.put(tier.getKey(), tier);
        }

        public void lock_(Tier tier)
        {
            getSync.getSync(tier.getReadWriteLock()).lock();
        }

        public void unlock_(Tier tier)
        {
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }

        public void release()
        {
            Iterator<Tier> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier tier = (Tier) lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
        }

        public void releaseAndClear()
        {
            Iterator<Tier> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier tier = (Tier) lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
            mapOfLockedTiers.clear();
        }

        private void exclusive()
        {
            Iterator<Tier> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier tier = (Tier) lockedTiers.next();
                tier.getReadWriteLock().writeLock().lock();
            }
            getSync = new WriteLockExtractor();
        }

        public void downgrade()
        {
            if (getSync.isExeclusive())
            {
                Iterator<Tier> lockedTiers = mapOfLockedTiers.values().iterator();
                while (lockedTiers.hasNext())
                {
                    Tier tier = (Tier) lockedTiers.next();
                    tier.getReadWriteLock().readLock().lock();
                    tier.getReadWriteLock().writeLock().unlock();
                }
                getSync = new ReadLockExtractor();
            }
        }

        public void upgrade()
        {
            if (getSync.isExeclusive())
            {
                throw new IllegalStateException();
            }
            release();
            exclusive();
        }

        public boolean upgrade(Level levelOfChild)
        {
            if (!getSync.isExeclusive())
            {
                release();
                // TODO Use Release and Clear.
                levelOfChild.release();
                levelOfChild.mapOfLockedTiers.clear();
                exclusive();
                levelOfChild.exclusive();
                return true;
            }
            else if (!levelOfChild.getSync.isExeclusive())
            {
                levelOfChild.release();
                levelOfChild.mapOfLockedTiers.clear();
                levelOfChild.exclusive();
                return true;
            }
            return false;
        }
    }

    public interface Deletable
    {
        public boolean deletable(Object object);
    }

    public final static Deletable ANY = new Deletable()
    {
        public boolean deletable(Object object)
        {
            return true;
        }
    };

    public final static class Query
    {
        private final Object txn;

        private final Map<Object, Tier> mapOfDirtyTiers;

        private final Strata strata;

        public Query(Object txn, Strata strata, Map<Object, Tier> mapOfDirtyTiers)
        {
            this.txn = txn;
            this.mapOfDirtyTiers = mapOfDirtyTiers;
            this.strata = strata;
        }

        public Strata getStrata()
        {
            return strata;
        }

        private void write()
        {
            Iterator<Tier> tiers = mapOfDirtyTiers.values().iterator();
            while (tiers.hasNext())
            {
                Tier tier = (Tier) tiers.next();
                tier.write(txn);
            }
            mapOfDirtyTiers.clear();

            strata.structure.getStorage().commit(txn);
        }

        private void testInnerTier(Mutation mutation, Decision subsequent, Decision swap, Level levelOfParent, Level levelOfChild, InnerTier parent, int rewind)
        {
            boolean tiers = subsequent.test(mutation, levelOfParent, levelOfChild, parent);
            boolean keys = swap.test(mutation, levelOfParent, levelOfChild, parent);
            if (tiers || keys)
            {
                if (!levelOfParent.getSync.isExeclusive() || !levelOfChild.getSync.isExeclusive())
                {
                    levelOfParent.upgrade(levelOfChild);
                    levelOfParent.listOfOperations.clear();
                    levelOfChild.listOfOperations.clear();
                    testInnerTier(mutation, subsequent, swap, levelOfParent, levelOfChild, parent, rewind);
                }
                else if (!tiers)
                {
                    mutation.rewind(rewind);
                }
            }
            else
            {
                mutation.rewind(rewind);
            }
        }

        /**
         * Both {@link #insert inert()} and {@link #remove remove()}  use this
         * generalized mutation method that implements locking the proper
         * tiers during the descent of the tree to find the leaf to mutate.
         * <p>
         * This generalized mutation will insert or remove a single item.
         *
         * @param mutation An object that maintains the state of this insert
         * or delete.
         * @param initial A decision to split or merge the root.
         * @param subsequent A decision to split, merge or delete an inner
         * tier that is not the root tier.
         * @param swap For remove, determine if the object removed is an inner
         * tier pivot and needs to be swapped.
         * @param penultimate A decision about the both the inner tier that
         * references leaves and the leaf tier itself, whether to split, merge
         * or delete the leaf, the insert or delete action to take on the
         * leaf, or whether to restart the descent.
         */
        private Object generalized(Mutation mutation, RootDecision initial,
            Decision subsequent, Decision swap, Decision penultimate)
        {
            // TODO Replace this with our caching pattern.

            // Inform the tier cache that we are about to perform a mutation
            // of the tree.
            if (mapOfDirtyTiers.size() == 0)
            {
                strata.writeMutex.lock();
            }

            Storage storage = strata.structure.getStorage();
            synchronized (this)
            {
                // FIXME Not going to happen. Right?
                if (storage.isKeyNull(strata.rootKey))
                {
                    if (mutation.deletable != null)
                    {
                        if (mapOfDirtyTiers.size() == 0)
                        {
                            strata.writeMutex.unlock();
                        }
                        return null;
                    }

                    InnerTier root = storage.newInnerTier(strata.structure, txn, LEAF);
                    LeafTier leaf = storage.newLeafTier(strata.structure, txn);
                    root.add(new Branch(leaf.getKey(), null));

                    mapOfDirtyTiers.put(root.getKey(), root);
                    mapOfDirtyTiers.put(leaf.getKey(), leaf);

                    if (mapOfDirtyTiers.size() > mutation.structure.getSchema().getMaxDirtyTiers())
                    {
                        write();
                    }
                }
            }

            mutation.listOfLevels.add(new Level(false));

            InnerTier parent = getRoot();
            Level levelOfParent = new Level(false);
            levelOfParent.lockAndAdd(parent);
            mutation.listOfLevels.add(levelOfParent);

            Level levelOfChild = new Level(false);
            mutation.listOfLevels.add(levelOfChild);

            if (initial.test(mutation, levelOfParent, parent))
            {
                levelOfParent.upgrade(levelOfChild);
                if (initial.test(mutation, levelOfParent, parent))
                {
                    initial.operation(mutation, levelOfParent, parent);
                }
                else
                {
                    mutation.rewind(0);
                }
            }

            for (;;)
            {
                if (parent.getChildType() == INNER)
                {
                    testInnerTier(mutation, subsequent, swap, levelOfParent, levelOfChild, parent, 0);
                    Branch branch = parent.find(mutation.txn, mutation.fields);
                    InnerTier child = (InnerTier) parent.getTier(txn, branch.getRightKey());
                    parent = child;
                }
                else
                {
                    testInnerTier(mutation, penultimate, swap, levelOfParent, levelOfChild, parent, 1);
                    break;
                }
                levelOfParent = levelOfChild;
                levelOfChild = new Level(levelOfChild.getSync.isExeclusive());
                mutation.listOfLevels.add(levelOfChild);
                mutation.shift();
            }

            if (mutation.leafOperation.operate(mutation, levelOfChild))
            {
                ListIterator<Level> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
                while (levels.hasPrevious())
                {
                    Level level = (Level) levels.previous();
                    ListIterator<Operation> operations = level.listOfOperations.listIterator(level.listOfOperations.size());
                    while (operations.hasPrevious())
                    {
                        Operation operation = (Operation) operations.previous();
                        operation.operate(mutation);
                    }
                }

                if (mutation.mapOfDirtyTiers.size() > mutation.structure.getSchema().getMaxDirtyTiers())
                {
                    write();
                }
            }

            ListIterator<Level> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
            while (levels.hasPrevious())
            {
                Level level = (Level) levels.previous();
                level.releaseAndClear();
            }

            if (mapOfDirtyTiers.size() == 0)
            {
                strata.writeMutex.unlock();
            }

            return mutation.mapOfVariables.get(RESULT);
        }

        // FIXME Rename add.
        // FIXME Key of object?
        public void insert(Object keyOfObject)
        {
            Comparable<?>[] fields = strata.structure.getSchema().getFieldExtractor().getFields(txn, keyOfObject);
            Object bucket = strata.structure.newBucket(fields, keyOfObject);
            Mutation mutation = new Mutation(strata.structure, txn, mapOfDirtyTiers, bucket, fields, null);
            generalized(mutation, new SplitRoot(), new SplitInner(), new InnerNever(), new LeafInsert());
        }

        public Object remove(Object keyOfObject)
        {
            Comparable<?>[] fields = strata.structure.getSchema().getFieldExtractor().getFields(txn, keyOfObject);
            return remove(fields, ANY);
        }

        // TODO Where do I actually use deletable? Makes sense, though. A
        // condition to choose which to delete.
        public Object remove(Comparable<?>[] fields, Deletable deletable)
        {
            Mutation mutation = new Mutation(strata.structure, txn, mapOfDirtyTiers, null, fields, deletable);
            do
            {
                mutation.listOfLevels.clear();

                Object search = mutation.mapOfVariables.get(SEARCH);
                mutation.mapOfVariables.clear();
                if (search != null)
                {
                    mutation.mapOfVariables.put(SEARCH, search);
                }

                generalized(mutation, new DeleteRoot(), new MergeInner(), new SwapKey(), new LeafRemove());
            }
            while (mutation.mapOfVariables.containsKey(SEARCH));

            Object removed = mutation.mapOfVariables.get(RESULT);

            return removed;
        }

        public Cursor find(Object keyOfObject)
        {
            return find(strata.structure.getSchema().getFieldExtractor().getFields(txn, keyOfObject));
        }

        // Here is where I get the power of not using comparator.
        public Cursor find(Comparable<?>... fields)
        {
            Lock previous = new ReentrantLock();
            previous.lock();
            InnerTier tier = getRoot();
            for (;;)
            {
                tier.getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = tier.getReadWriteLock().readLock();
                Branch branch = tier.find(txn, fields);
                if (tier.getChildType() == LEAF)
                {
                    LeafTier leaf = strata.structure.getStorage().getLeafTier(strata.structure, txn, branch.getRightKey());
                    leaf.getReadWriteLock().readLock().lock();
                    previous.unlock();
                    return leaf.find(txn, fields);
                }
                tier = (InnerTier) tier.getTier(txn, branch.getRightKey());
            }
        }

        public Cursor first()
        {
            Branch branch = null;
            InnerTier tier = getRoot();
            Lock previous = new ReentrantLock();
            previous.lock();
            for (;;)
            {
                tier.getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = tier.getReadWriteLock().readLock();
                branch = tier.get(0);
                if (tier.getChildType() == LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.getTier(txn, branch.getRightKey());
            }
            LeafTier leaf = (LeafTier) tier.getTier(txn, branch.getRightKey());
            leaf.getReadWriteLock().readLock().lock();
            previous.unlock();
            return new Cursor(strata.structure, txn, leaf, 0);
        }

        public Cursor last_UNIMPLEMENTED()
        {
            Branch branch = null;
            InnerTier tier = getRoot();
            Lock previous = new ReentrantLock();
            previous.lock();
            for (;;)
            {
                tier.getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = tier.getReadWriteLock().readLock();
                branch = tier.get(tier.getSize());
                if (tier.getChildType() == LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.getTier(txn, branch.getRightKey());
            }
            LeafTier leaf = strata.structure.getStorage().getLeafTier(strata.structure, txn, branch.getRightKey());
            leaf.getReadWriteLock().readLock().lock();
            previous.unlock();
            return new Cursor(strata.structure, txn, leaf, leaf.getSize());
        }

        public void flush()
        {
            if (mapOfDirtyTiers.size() != 0)
            {
                write();
                strata.writeMutex.unlock();
            }
        }

        private void destroy(InnerTier inner)
        {
            if (inner.getChildType() == INNER)
            {
                Iterator<Branch> branches = inner.listIterator();
                while (branches.hasNext())
                {
                    Branch branch = (Branch) branches.next();
                    destroy(strata.structure.getStorage().getInnerTier(strata.structure, txn, branch.getRightKey()));
                }
            }
            else
            {
                Iterator<Branch> branches = inner.listIterator();
                while (branches.hasNext())
                {
                    Branch branch = (Branch) branches.next();
                    LeafTier leaf = strata.structure.getStorage().getLeafTier(strata.structure, txn, branch.getRightKey());
                    strata.structure.getStorage().free(strata.structure, txn, leaf);
                }
            }
            strata.structure.getStorage().free(strata.structure, txn, inner);
        }

        public void destroy()
        {
            // FIXME Get the write mutex.
            synchronized (this)
            {
                if (strata.structure.getStorage().isKeyNull(strata.rootKey))
                {
                    return;
                }
            }
            destroy(getRoot());
        }

        public void copacetic()
        {
            // FIXME Lock.
            Comparator<Object> comparator = new CopaceticComparator(txn, strata.structure);
            getRoot().copacetic(txn, new Copacetic(comparator));
            Cursor cursor = first();
            Object previous = cursor.next();
            while (cursor.hasNext())
            {
                Object next = cursor.next();
                if (comparator.compare(previous, next) > 0)
                {
                    throw new IllegalStateException();
                }
                previous = next;
            }
        }

        private InnerTier getRoot()
        {
            return strata.structure.getStorage().getInnerTier(strata.structure, txn, strata.rootKey);
        }
    }

    public final static class Cursor
    {
        private final Structure structure;

        private final Object txn;

        private int index;

        private LeafTier leaf;

        private boolean released;

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
            if (released)
            {
                throw new IllegalStateException();
            }
            if (index == leaf.getSize())
            {
                if (structure.getStorage().isKeyNull(leaf.getNextLeafKey()))
                {
                    throw new IllegalStateException();
                }
                LeafTier next = structure.getStorage().getLeafTier(structure, txn, leaf.getNextLeafKey());
                next.getReadWriteLock().readLock().lock();
                leaf.getReadWriteLock().readLock().unlock();
                leaf = next;
                index = 0;
            }
            Object object = structure.getObjectKey(leaf.get(index++));
            if (!hasNext())
            {
                release();
            }
            return object;
        }

        public void release()
        {
            if (!released)
            {
                leaf.getReadWriteLock().readLock().unlock();
                released = true;
            }
        }
    }

    public static class Copacetic
    {
        private final Set<Object> seen;

        private Copacetic(Comparator<Object> comparator)
        {
            this.seen = new TreeSet<Object>(comparator);
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
