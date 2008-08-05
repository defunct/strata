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

    public final static long INNER = 1;

    public final static long LEAF = 2;
    
    public final static short EMPTY_CACHE_TYPE = 1;
    
    public final static short PER_QUERY_CACHE_TYPE = 2;
    
    public final static short PER_STRATA_CACHE_TYPE = 3;

    private final static Object RESULT = new Object();

    private final static Object REPLACEMENT = new Object();

    private final static Object DELETING = new Object();

    private final static Object LEFT_LEAF = new Object();

    private final static Object SEARCH = new Object();

    private final Object rootKey;
    
    private final Structure structure;
    
    private final Storage<Object> storage;
    
    private transient TierCache<Object> cache;

    private Strata(Schema creator, Object txn)
    {
        this.structure = new Structure(creator);
        this.storage = creator.getStorageSchema().newStorage(structure, txn);
        this.cache = createCache(structure, storage);

        InnerTier root = new InnerTier(storage.getBranchStore().newTier(txn));
        root.setChildType(LEAF);
        LeafTier leaf = new LeafTier(storage.getLeafStore().newTier(txn));
        root.add(new Branch(leaf.getTier().getKey(), null));

        cache.begin();
        
        cache.getBranchSet().dirty(txn, root.getTier());
        cache.getLeafSet().dirty(txn, leaf.getTier());
        
        cache.end(txn);
        
        this.rootKey = root.getTier().getKey();
    }

    public Object getRootKey()
    {
        return rootKey;
    }

    public Schema getSchema()
    {
        return new Schema(structure.getSchema());
    }

    public Query query(Object txn)
    {
        return new Query(txn, this, cache.newTierCache());
    }
    
    private static <T> TierCache<T> createCache(Structure structure, Storage<T> storage)
    {
        switch (structure.getSchema().getCacheType())
        {
            case EMPTY_CACHE_TYPE:
                return new EmptyTierCache<T>(storage);
            case PER_QUERY_CACHE_TYPE:
                return new PerQueryTierCache<T>(storage, 16);
            case PER_STRATA_CACHE_TYPE:
                return new CommonTierCache<T>(storage, 16);
        }
        throw new IllegalArgumentException();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        cache = createCache(structure, storage);
    }
    
    @SuppressWarnings("unchecked")
    private final static int compare(Object left, Object right)
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

        private Storage.Schema<Object> storageSchema;

        private Extractor extractor;

        private short cacheType;

        private int leafSize;
        
        private int innerSize;

        private boolean cacheFields;

        private int maxDirtyTiers;

        public Schema()
        {
            this.storageSchema = new ArrayListStorage.Schema<Object>();
            this.extractor = new ComparableExtractor();
            this.leafSize = 5;
            this.innerSize = 5;
            this.cacheFields = false;
            this.maxDirtyTiers = 0;
            this.cacheType = PER_STRATA_CACHE_TYPE;
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
            Strata strata = new Strata(this, txn);
            return new Query(txn, strata, strata.cache.newTierCache());
        }
        
        public short getCacheType()
        {
            return cacheType;
        }

        public void setCacheType(short cacheType)
        {
            this.cacheType = cacheType;
        }

        public Storage.Schema<Object> getStorageSchema()
        {
            return storageSchema;
        }
        
        public void setStorageSchema(Storage.Schema<Object> storageSchema)
        {
            this.storageSchema = storageSchema;
        }
        
        public void setFieldExtractor(Extractor extractor)
        {
            this.extractor = extractor;
        }

        public Extractor getFieldExtractor()
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

        private final Cooper cooper;

        public Structure(Schema schema)
        {
            this.schema = schema;
            this.cooper = schema.getCacheFields() ? (Cooper) new BucketCooper() : (Cooper) new LookupCooper();

        }

        public Schema getSchema()
        {
            return schema;
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

    public interface Extractor
    {
        public void extract(Object txn, Object object, Record record);
    }
    
    public interface Record
    {
        public void fields(Comparable<?>... comparables);
    }
    
    private final static class CoreRecord
    implements Record
    {
        private Comparable<?>[] comparables;

        public void fields(Comparable<?>... comparables)
        {
            this.comparables = comparables;
        }
        
        public Comparable<?>[] getComparables()
        {
            return comparables;
        }
    }

    public final static class ComparableExtractor
    implements Extractor, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        @SuppressWarnings("unchecked")
        private Comparable cast(Object object)
        {
            if (!(object instanceof Comparable<?>))
            {
                System.out.println(object.getClass());
            }
            return (Comparable) object;
        }
        
        public void extract(Object txn, Object object, Record record)
        {
            record.fields(cast(object));
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
        public Object newBucket(Object txn, Extractor fields, Object keyOfObject);

        public Object newBucket(Comparable<?>[] fields, Object keyOfObject);

        public Object getObjectKey(Object object);

        public Comparable<?>[] getFields(Object txn, Extractor extractor, Object object);

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

        public Object newBucket(Object txn, Extractor extractor, Object object)
        {
            CoreRecord record = new CoreRecord();
            extractor.extract(txn, object, record);
            return new Bucket(record.getComparables(), object);
        }

        public Object newBucket(Comparable<?>[] fields, Object keyOfObject)
        {
            return new Bucket(fields, keyOfObject);
        }

        public Comparable<?>[] getFields(Object txn, Extractor extractor, Object object)
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

        public Object newBucket(Object txn, Extractor extractor, Object keyOfObject)
        {
            return keyOfObject;
        }

        public Object newBucket(Comparable<?>[] comparables, Object keyOfObject)
        {
            return keyOfObject;
        }

        public Comparable<?>[] getFields(Object txn, Extractor extractor, Object object)
        {
            CoreRecord record = new CoreRecord();
            extractor.extract(txn, object, record);
            return record.getComparables();
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

    public static class Tier<T>
    extends ArrayList<T>
    {
        private final static long serialVersionUID = 20080621L;

        private final Structure structure;
        
        private final Identifier<T> identifier;
        
        private final ReadWriteLock readWriteLock;
        
        private Object storageData;
        
        private Object value;
        
        public Tier(Structure structure, Identifier<T> identifier, Object storageData, int size)
        {
            super(size);
            this.structure = structure;
            this.identifier = identifier;
            this.readWriteLock = new ReentrantReadWriteLock();
            this.storageData = storageData;
        }
        
        public Identifier<T> getIdentifier()
        {
            return identifier;
        }
        
        public Structure getStructure()
        {
            return structure;
        }
        
        public ReadWriteLock getReadWriteLock()
        {
            return readWriteLock;
        }
        
        public Object getValue()
        {
            return value;
        }
        
        public void setValue(Object value)
        {
            this.value = value;
        }

        public Object getKey()
        {
            return identifier.getKey(this);
        }

        public Object getStorageData()
        {
            return storageData;
        }
        
        public void setStorageData(Object storageData)
        {
            this.storageData = storageData;
        }
    }
    
    public final static class LeafTier
    {
        private Tier<Object> tier;

        public LeafTier(Tier<Object> tier)
        {
            this.tier = tier;
        }
        
        public Tier<Object> getTier()
        {
            return tier;
        }

        public Object getNextLeafKey()
        {
            return tier.getValue();
        }

        public void setNextLeafKey(Object nextLeafKey)
        {
            tier.setValue(nextLeafKey);
        }

        public void add(Object txn, Object keyOfObject)
        {
            tier.add(tier.getStructure().newBucket(txn, keyOfObject));
        }

        public void addBucket(Object bucket)
        {
            tier.add(bucket);
        }

        public Object remove(int index)
        {
            return tier.remove(index);
        }

        public Object get(int index)
        {
            return tier.get(index);
        }

        public LeafTier getNextAndLock(Navigator navigator, Level levelOfLeaf)
        {
            if (!tier.getIdentifier().isKeyNull(getNextLeafKey()))
            {
                LeafTier leaf = navigator.getLeafTier(getNextLeafKey());
                levelOfLeaf.lockAndAdd(leaf.getTier());
                return leaf;
            }
            return null;
        }

        private LeafTier getNext(Navigator navigator)
        {
            return navigator.getLeafTier(getNextLeafKey());
        }

        public void link(Mutation mutation, LeafTier nextLeaf)
        {
            mutation.getCache().getLeafSet().dirty(mutation.getTxn(), getTier());
            mutation.getCache().getLeafSet().dirty(mutation.getTxn(), nextLeaf.getTier());
            Object nextLeafKey = getNextLeafKey();
            setNextLeafKey(nextLeaf.getTier().getKey());
            nextLeaf.setNextLeafKey(nextLeafKey);
        }

        public void append(Mutation mutation, Level levelOfLeaf)
        {
            if (tier.size() == tier.getStructure().getSchema().getLeafSize())
            {
                LeafTier nextLeaf = getNextAndLock(mutation, levelOfLeaf);
                if (null == nextLeaf || compare(mutation.fields, mutation.getFields(nextLeaf.get(0))) != 0)
                {
                    nextLeaf = mutation.newLeafTier();
                    link(mutation, nextLeaf);
                }
                nextLeaf.append(mutation, levelOfLeaf);
            }
            else
            {
                addBucket(mutation.bucket);
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), tier);
            }
        }

        public Cursor<Object> find(Navigator navigator, Comparable<?>[] fields)
        {
            for (int i = 0; i < tier.size(); i++)
            {
                int compare = compare(tier.getStructure().getFields(navigator.getTxn(), get(i)), fields);
                if (compare >= 0)
                {
                    return new Cursor<Object>(navigator, this, i);
                }
            }
            return new Cursor<Object>(navigator, this, tier.size());
        }

        public String toString()
        {
            return tier.toString();
        }

        public void copacetic(Navigator navigator, Copacetic copacetic)
        {
            if (tier.size() < 1)
            {
                throw new IllegalStateException();
            }

            if (tier.size() > tier.getStructure().getSchema().getLeafSize())
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Iterator<Object> objects = tier.listIterator();
            Comparator<Object> comparator = new CopaceticComparator(navigator.getTxn(), tier.getStructure());
            while (objects.hasNext())
            {
                Object object = objects.next();
                if (previous != null && comparator.compare(previous, object) > 0)
                {
                    throw new IllegalStateException();
                }
                previous = object;
            }
            if (!tier.getIdentifier().isKeyNull(getNextLeafKey())
                && comparator.compare(get(tier.size() - 1), getNext(navigator).get(0)) == 0
                && tier.getStructure().getSchema().getLeafSize() != tier.size() && comparator.compare(get(0), get(tier.size() - 1)) != 0)
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
    {
        private final Tier<Branch> tier;

        public InnerTier(Tier<Branch> tier)
        {
            this.tier = tier;
        }
        
        public Tier<Branch> getTier()
        {
            return tier;
        }

        public long getChildType()
        {
            return (Long) tier.getValue();
        }
        
        public void setChildType(long childType)
        {
            tier.setValue(childType);
        }

        public void add(Object txn, Object keyOfLeft, Object keyOfObject)
        {
            Object bucket = null;
            if (keyOfObject != null)
            {
                bucket = tier.getStructure().newBucket(txn, keyOfObject);
            }
            tier.add(new Branch(keyOfLeft, bucket));
        }

        public boolean add(Branch branch)
        {
            return tier.add(branch);
        }

        public void add(int index, Branch branch)
        {
            tier.add(index, branch);
        }

        public Branch remove(int index)
        {
            return tier.remove(index);
        }

        public ListIterator<Branch> listIterator()
        {
            return tier.listIterator();
        }

        public Branch get(int index)
        {
            return tier.get(index);
        }

        public Branch find(Object txn, Comparable<?>[] fields)
        {
            Iterator<Branch> branches = listIterator();
            Branch candidate = branches.next();
            while (branches.hasNext())
            {
                Branch branch = branches.next();
                if (compare(fields, tier.getStructure().getFields(txn, branch.getPivot())) < 0)
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
                Branch branch = branches.next();
                if (branch.getRightKey().equals(keyOfTier))
                {
                    return index;
                }
                index++;
            }
            return -1;
        }

        public void copacetic(Navigator navigator, Copacetic copacetic)
        {
            if (tier.size() < 0)
            {
                throw new IllegalStateException();
            }

            if (tier.size() > tier.getStructure().getSchema().getInnerSize())
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Object lastLeftmost = null;

            Comparator<Object> comparator = new CopaceticComparator(navigator.getTxn(), tier.getStructure());
            Iterator<Branch> branches = listIterator();
            boolean isMinimal = true;
            while (branches.hasNext())
            {
                Branch branch = branches.next();
                if (isMinimal != branch.isMinimal())
                {
                    throw new IllegalStateException();
                }
                isMinimal = false;

                if (getChildType() == INNER)
                {
                    navigator.getInnerTier(branch.getRightKey()).copacetic(navigator, copacetic);
                }
                else if (getChildType() == LEAF)
                {
                    navigator.getLeafTier(branch.getRightKey()).copacetic(navigator, copacetic);
                }
                else
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

                Object leftmost = getLeftMost(navigator, this, branch);
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

        private Object getLeftMost(Navigator navigator, InnerTier inner, Branch branch)
        {
            while (inner.getChildType() != LEAF)
            {
                inner = navigator.getInnerTier(branch.getRightKey());
                branch = inner.get(0);
            }
            return navigator.getLeafTier(branch.getRightKey()).get(0);
        }

        public String toString()
        {
            return tier.toString();
        }
    }

    public interface Identifier<T>
    {
        public Object getKey(Tier<T> tier);

        public Object getNullKey();

        public boolean isKeyNull(Object object);
    }

    public interface Storage<T>
    {
        public void commit(Object txn);
        
        public Store<Branch> getBranchStore();
        
        public Store<T> getLeafStore();
        
        public Schema<T> newSchema();
        
        public interface Schema<T>
        {
            public Storage<T> newStorage(Structure structure, Object txn);
        }
    }

    /**
     * Interface for the creation and storage of tiers.
     * <p>
     * Now, more than ever, we need to make a Tier class so that this
     * interface reads and writes a series of records, using a reader and
     * writer, rather than having separate classes for inner tier and leaf
     * tier.
     */
    public interface Store<T>
    extends Serializable
    {
        public Identifier<T> getIdentifier();
        
        public Tier<T> newTier(Object txn);

        public Tier<T> getTier(Object txn, Object key);

        public void write(Object txn, Tier<T> tier);

        public void free(Object txn, Tier<T> tier);
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

    interface TierSet<T>
    {
        
        /**
         * Record a tier as dirty in the tier cache.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(Object txn, Tier<T> tier);
        
        /**
         * Remove a dirty tier from the tier cache.
         * 
         * @param tier The tier to remove.
         */
        public void remove(Tier<T> tier);
        
        public void write(Object txn);
        
        public int size();
    }

    /**
     * A strategy for both caching dirty tiers in order to writing them out to
     * storage in a batch as well as for locking the Strata for exclusive
     * insert and delete.
     */
    interface TierCache<T>
    {
        public Storage<T> getStorage();
        
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
         * Notify the tier cache that an insert or delete is about to begin
         * so that the tier cache can acquire locks if necessary.
         */
        public void begin();

        public TierSet<Branch> getBranchSet();
        
        public TierSet<T> getLeafSet();
        
        /**
         * Notify the tier cache that an insert or delete has completed and
         * so that the tier cache can determine if the cache should be flushed. 
         * If the tier cache is flushed and the auto commit property is true,
         * the tier cache will call the commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void end(Object txn);
        
        /**
         * Flush any dirty pages in the tier cache and empty the tier cache.
         * If the auto commit property is true, the tier cache will call the
         * commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void flush(Object txn);
        
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
        public TierCache<T> newTierCache();
    }
    
    interface AutoCommit
    {
        public void autoCommit(Object txn);
    }
 
    public static class EmptyTierSet<T>
    implements TierSet<T>
    {
        private AutoCommit autoCommit;
        
        private Store<T> store;
        
        public EmptyTierSet(AutoCommit autoCommit, Store<T> store)
        {
            this.autoCommit = autoCommit;
            this.store = store;
        }
        
        /**
         * For the empty tier cache, this method immediately writes the dirty
         * tier to storage and commits the write if auto commit is enabled.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(Object txn, Tier<T> tier)
        {
            store.write(txn, tier);
            autoCommit.autoCommit(txn);
        }
        
        /**
         * For the empty tier cache, this method does nothing.
         * 
         * @param tier The tier to remove.
         */
        public void remove(Tier<T> tier)
        {
        }
        
        public void write(Object txn)
        {
        }
        
        public int size()
        {
            return 0;
        }
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
    public static class EmptyTierCache<T>
    implements TierCache<T>, AutoCommit
    {
        /**
         * A lock instance that will exclusively lock the Strata for insert and
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
        
        private final Storage<T> storage;
        
        private final EmptyTierSet<Branch> branchTierSet;
        
        private final EmptyTierSet<T> leafTierSet;

        public EmptyTierCache(Storage<T> storage)
        {
            this(storage, new ReentrantLock(), true);
        }
        
        /**
         * Create an empty tier cache with 
         */
        protected EmptyTierCache(Storage<T> storage, Lock lock, boolean autoCommit)
        {
            this.storage = storage;
            this.lock = lock;
            this.autoCommit = autoCommit;
            this.branchTierSet = new EmptyTierSet<Branch>(this, storage.getBranchStore());
            this.leafTierSet = new EmptyTierSet<T>(this, storage.getLeafStore());
        }

        public void autoCommit(Object txn)
        {
            if (isAutoCommit())
            {
                storage.commit(txn);
            }
        }
        
        public Storage<T> getStorage()
        {
            return storage;
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
         * Lock the strata exclusive for inserts and deletes. This does not
         * prevent other threads from reading the strata.
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

        public TierSet<Branch> getBranchSet()
        {
            return branchTierSet;
        }
        
        public TierSet<T> getLeafSet()
        {
            return leafTierSet;
        }
        
        /**
         * A noop implementation of storage synchronization called after an
         * insert or delete of an object from the strata.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void end(Object txn)
        {
        }
          
        /**
         * Since the cache is always empty, this method merely calls the
         * commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void flush(Object txn)
        {
            autoCommit(txn);
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
        public TierCache<T> newTierCache()
        {
            return new EmptyTierCache<T>(getStorage(), lock, autoCommit);
        }
    }
    
    static class BasicTierSet<T>
    implements TierSet<T>
    {
        private final Object mutex;
        
        private final Store<T> store;
        
        private final Map<Object, Tier<T>> mapOfTiers;
        
        public BasicTierSet(Store<T> store, Object mutex)
        {
            this.store = store;
            this.mutex = mutex;
            this.mapOfTiers = new HashMap<Object, Tier<T>>();
        }
        
        /**
         * Adds dirty tier to the cache so that it can be flushed when an
         * insert or delete completes if the maximum count of dirty tiers has
         * been reached.
         * 
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(Object txn, Tier<T> tier)
        {
            synchronized (mutex)
            {
                mapOfTiers.put(tier.getKey(), tier);
            }
        }
        
        /**
         * Removes a dirty tier from the tier cache so that it will not be
         * written when the cache is flushed. Always remove a tier from the
         * dirty tier cache when you free the tier.
         * 
         * @param tier The tier to remove.
         */
        public void remove(Tier<T> tier)
        {
            synchronized (mutex)
            {
                mapOfTiers.remove(tier.getKey());
            }
        }
        
        public void write(Object txn)
        {
            Iterator<Tier<T>> tiers = mapOfTiers.values().iterator();
            while (tiers.hasNext())
            {
                Tier<T> tier = tiers.next();
                store.write(txn, tier);
            }

            mapOfTiers.clear();
        }
        
        public int size()
        {
            return mapOfTiers.size();
        }
    }
    
    /**
     * Keeps a synchronized map of dirty tiers with a maximum size at which
     * the tiers are written to file and flushed. Used as the base class of
     * both the per query and per strata implementations of the tier cache.
     */
    static class AbstractTierCache<T>
    extends EmptyTierCache<T>
    {
        protected final Object mutex;
        
        protected final TierSet<Branch> branchTierSet;
        
        protected final TierSet<T> leafTierSet;
        
        /**
         * The dirty tier cache size that when reached, will cause the cache
         * to empty and the tiers to be written.
         */
        protected final int max;

        /**
         * Create a tier cache using the specified map of dirty tiers and the
         * that flushes when the maximum size is reached. The lock is an
         * exclusive lock on the strata.
         *
         * @param lock An exclusive lock on the Strata.
         * @param mapOfDirtyTiers The map of dirty tiers.
         * @param max The dirty tier cache size that when reached, will cause
         * the cache to empty and the tiers to be written.
         * @param autoCommit If true, the commit method of the storage
         * strategy is called after the dirty tiers are written.
         */
        public AbstractTierCache(Storage<T> storage, Lock lock, Object mutex, TierSet<Branch> branchTierSet, TierSet<T> leafTierSet, int max, boolean autoCommit)
        {
            super(storage, lock, autoCommit);
            this.mutex = mutex;
            this.max = max;
            this.branchTierSet = branchTierSet;
            this.leafTierSet = leafTierSet;
        }
        
        /**
         * Empty the dirty tier cache by writing out the dirty tiers and
         * clearing the map of dirty tiers. If force is true, we do not
         * check that the maximum size has been reached.  If the auto commit
         * is true, then the commit method of the storage strategy is called.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param force If true save unconditionally, do not check the
         * maximum size.
         */
        protected void save(Object txn, boolean force)
        {
            synchronized (mutex)
            {
                if (force || branchTierSet.size() + leafTierSet.size() >= max)
                {
                    branchTierSet.write(txn);
                    leafTierSet.write(txn);
                    if (isAutoCommit())
                    {
                        getStorage().commit(txn);
                    }
                }
            }
        }
        
        /**
         * Empty the dirty tier cache by writing out the dirty tiers and
         * clearing the map of dirty tiers.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        @Override
        public void flush(Object txn)
        {
            save(txn, true);
        }
    }
     
    /**
     * A tier cache that maintains a map of dirty tiers per query and that
     * prevents other queries from inserting or deleting items until the tier
     * cache is emptied and dirty tiers are persisted.
     * <p>
     * The exclusive strata lock will be called when an insert or delete
     * begins and the map of dirty tiers is empty. At the end of an insert or
     * delete, if the map of dirty tiers is empty, the exclusive strata lock
     * is released. Thus, the empty map of dirty tiers is an indicator that
     * the associated query does not hold the lock on the strata.
     */
    public static class PerQueryTierCache<T>
    extends AbstractTierCache<T>
    {
        /**
         * Create a per query tier cache.
         *
         * @param lock An exclusive lock on the Strata.
         * @param max The dirty tier cache size that when reached, will cause
         * the cache to empty and the tiers to be written.
         */
        public PerQueryTierCache(Storage<T> storage, int max)
        {
            this(storage, new ReentrantLock(), max, true);
        }
        
        private PerQueryTierCache(Storage<T> storage, Lock lock, int max, boolean autoCommit)
        {
            super(storage, lock, new Object(), new BasicTierSet<Branch>(storage.getBranchStore(), new Object()), new BasicTierSet<T>(storage.getLeafStore(), new Object()), max, autoCommit);
        }
        
        public void begin()
        {
            if (branchTierSet.size() + leafTierSet.size() == 0)
            {
                lock();
            }
        }
        
        public void end(Object txn)
        {
            save(txn, false);
            if (branchTierSet.size() + leafTierSet.size() == 0)
            {
                unlock();
            }
        }
        
        public TierCache<T> newTierCache()
        {
            return new PerQueryTierCache<T>(getStorage(), lock, max, isAutoCommit());
        }
    }
    
    public static class CommonTierCache<T>
    extends AbstractTierCache<T>
    {
        private final ReadWriteLock readWriteLock;

        public CommonTierCache(Storage<T> storage, int max)
        {
            this(storage, new ReentrantReadWriteLock(), new Object(), max);
        }

        private CommonTierCache(Storage<T> storage, ReadWriteLock readWriteLock, Object mutex, int max)
        {
            this(storage, readWriteLock, mutex, new BasicTierSet<Branch>(storage.getBranchStore(), mutex), new BasicTierSet<T>(storage.getLeafStore(), mutex), max, true);
        }

        private CommonTierCache(Storage<T> storage, ReadWriteLock readWriteLock, Object mutex, TierSet<Branch> branchTierSet, TierSet<T> leafTierSet, int max, boolean autoCommit)
        {
            super(storage, readWriteLock.writeLock(), mutex, branchTierSet, leafTierSet, max, autoCommit);
            this.readWriteLock = readWriteLock;
        }
        
        public void begin()
        {
            if (lockCount == 0)
            {
                readWriteLock.readLock().lock();
            }
        }
        
        public void end(Object txn)
        {
            save(txn, false);
            if (lockCount == 0)
            {
                readWriteLock.readLock().unlock();
            }
        }
        
        public TierCache<T> newTierCache()
        {
            return new CommonTierCache<T>(getStorage(), readWriteLock, mutex, branchTierSet, leafTierSet, max, isAutoCommit());
        }
    }
    
    private static class Navigator
    {
        private final Structure structure;
        
        private final Storage<Object> storage;

        private final Object txn;

        public Navigator(Structure structure, Storage<Object> storage, Object txn)
        {
            this.structure = structure;
            this.storage = storage;
            this.txn = txn;
        }
        
        public Structure getStructure()
        {
            return structure;
        }
        
        public Storage<Object> getStorage()
        {
            return storage;
        }
        
        public Object getTxn()
        {
            return txn;
        }
        
        public InnerTier getInnerTier(Object key)
        {
            return new InnerTier(storage.getBranchStore().getTier(txn, key));
        }
        
        public LeafTier getLeafTier(Object key)
        {
            return new LeafTier(storage.getLeafStore().getTier(txn, key));
        }
    }
    
    private final static class Mutation
    extends Navigator
    {
        public final Comparable<?>[] fields;

        public final Deletable deletable;

        public final LinkedList<Level> listOfLevels = new LinkedList<Level>();

        public final Map<Object, Object> mapOfVariables = new HashMap<Object, Object>();

        public final TierCache<Object> cache;

        public LeafOperation leafOperation;

        public final Object bucket;

        public Mutation(Navigator navigator, TierCache<Object> cache, Object bucket, Comparable<?>[] fields, Deletable deletable)
        {
            super(navigator.getStructure(), navigator.getStorage(), navigator.getTxn());
            this.cache = cache;
            this.fields = fields;
            this.deletable = deletable;
            this.bucket = bucket;
        }
        
        public TierCache<Object> getCache()
        {
            return cache;
        }
        
        public InnerTier newInnerTier(long childType)
        {
            InnerTier inner = new InnerTier(getStorage().getBranchStore().newTier(getTxn()));
            inner.setChildType(childType);
            return inner;
        }
        
        public LeafTier newLeafTier()
        {
            return new LeafTier(getStorage().getLeafStore().newTier(getTxn()));
        }
        
        public void rewind(int leaveExclusive)
        {
            Iterator<Level>levels = listOfLevels.iterator();
            int size = listOfLevels.size();
            boolean unlock = true;

            for (int i = 0; i < size - leaveExclusive; i++)
            {
                Level level = levels.next();
                Iterator<Operation> operations = level.listOfOperations.iterator();
                while (operations.hasNext())
                {
                    Operation operation = operations.next();
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
            return getStructure().getFields(getTxn(), key);
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
            return mutation.getStructure().getSchema().getInnerSize() == root.getTier().size();
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
                InnerTier left = mutation.newInnerTier(root.getChildType());
                InnerTier right = mutation.newInnerTier(root.getChildType());
                
                int partition = root.getTier().size() / 2;
                int fullSize = root.getTier().size();
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

                root.add(new Branch(left.getTier().getKey(), null));
                root.add(new Branch(right.getTier().getKey(), pivot));

                root.setChildType(INNER);

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), root.getTier());
                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), left.getTier());
                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), right.getTier());
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
            Branch branch = parent.find(mutation.getTxn(), mutation.fields);
            InnerTier child = mutation.getInnerTier(branch.getRightKey());
            levelOfChild.lockAndAdd(child.getTier());
            if (child.getTier().size() == mutation.getStructure().getSchema().getInnerSize())
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
                InnerTier right = mutation.newInnerTier(child.getChildType());

                int partition = child.getTier().size() / 2;

                while (partition < child.getTier().size())
                {
                    right.add(child.remove(partition));
                }

                Object pivot = right.get(0).getPivot();
                right.get(0).setPivot(null);

                int index = parent.getIndexOfTier(child.getTier().getKey());
                parent.add(index + 1, new Branch(right.getTier().getKey(), pivot));

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), parent.getTier());
                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), child.getTier());
                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), right.getTier());
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
            Branch branch = parent.find(mutation.getTxn(), mutation.fields);
            LeafTier leaf = mutation.getLeafTier(branch.getRightKey());
            levelOfChild.getSync = new WriteLockExtractor();
            levelOfChild.lockAndAdd(leaf.getTier());
            if (leaf.getTier().size() == mutation.getStructure().getSchema().getLeafSize())
            {
                Comparable<?>[] first = mutation.getFields(leaf.get(0));
                Comparable<?>[] last = mutation.getFields(leaf.get(leaf.getTier().size() - 1));
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
                Branch branch = inner.find(mutation.getTxn(), mutation.fields);
                LeafTier leaf = mutation.getLeafTier(branch.getRightKey());

                LeafTier right = mutation.newLeafTier();
                while (leaf.getTier().size() != 0)
                {
                    right.addBucket(leaf.remove(0));
                }

                leaf.link(mutation, right);

                int index = inner.getIndexOfTier(leaf.getTier().getKey());
                if (index != 0)
                {
                    throw new IllegalStateException();
                }
                inner.add(index + 1, new Branch(right.getTier().getKey(), right.get(0)));

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), inner.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), leaf.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), right.getTier());

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
                return last.getTier().getIdentifier().isKeyNull(last.getNextLeafKey()) || compare(mutation.getFields(last.getNext(mutation).get(0)), mutation.getFields(last.get(0))) != 0;
            }

            public boolean operate(Mutation mutation, Level levelOfLeaf)
            {
                Branch branch = inner.find(mutation.getTxn(), mutation.fields);
                LeafTier leaf = mutation.getLeafTier(branch.getRightKey());

                LeafTier last = leaf;
                while (!endOfList(mutation, last))
                {
                    last = last.getNextAndLock(mutation, levelOfLeaf);
                }

                LeafTier right = mutation.newLeafTier();
                last.link(mutation, right);

                inner.add(inner.getIndexOfTier(leaf.getTier().getKey()) + 1, new Branch(right.getTier().getKey(), mutation.bucket));

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), inner.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), leaf.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), right.getTier());

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
                Branch branch = inner.find(mutation.getTxn(), mutation.fields);
                LeafTier leaf = mutation.getLeafTier(branch.getRightKey());

                int middle = leaf.getTier().size() >> 1;
                boolean odd = (leaf.getTier().size() & 1) == 1;
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

                LeafTier right = mutation.newLeafTier();

                while (partition != leaf.getTier().size())
                {
                    right.addBucket(leaf.remove(partition));
                }

                leaf.link(mutation, right);

                int index = inner.getIndexOfTier(leaf.getTier().getKey());
                inner.add(index + 1, new Branch(right.getTier().getKey(), right.get(0)));

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), inner.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), leaf.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), right.getTier());
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
                Branch branch = inner.find(mutation.getTxn(), mutation.fields);
                LeafTier leaf = mutation.getLeafTier(branch.getRightKey());

                Object bucket = mutation.bucket;

                ListIterator<Object> objects = leaf.getTier().listIterator();
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

                // FIXME Now we are writing before we are splitting. Problem.
                // Empty cache does not work!
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), leaf.getTier());

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
            if (root.getChildType() == INNER && root.getTier().size() == 2)
            {
                InnerTier first = mutation.getInnerTier(root.get(0).getRightKey());
                InnerTier second = mutation.getInnerTier(root.get(1).getRightKey());
                // FIXME These numbers are off.
                return first.getTier().size() + second.getTier().size() == mutation.getStructure().getSchema().getInnerSize();
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
                if (root.getTier().size() != 0)
                {
                    throw new IllegalStateException();
                }

                InnerTier child = mutation.getInnerTier(root.remove(0).getRightKey());
                while (child.getTier().size() != 0)
                {
                    root.add(child.remove(0));
                }

                root.setChildType(child.getChildType());

                mutation.getCache().getBranchSet().remove(child.getTier());
                mutation.getStorage().getBranchStore().free(mutation.getTxn(), child.getTier());

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), root.getTier());
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
            Branch branch = parent.find(mutation.getTxn(), mutation.fields);
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
                    Branch branch = inner.find(mutation.getTxn(), mutation.fields);
                    branch.setPivot(replacement);
                    mutation.getCache().getBranchSet().dirty(mutation.getTxn(), inner.getTier());
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
     * If we encounter an inner tier has more than one child on the way down we
     * are not longer in the deleting state.
     * <p>
     * When we reach the leaf, if it has a size of one and we are in the
     * deleting state, then we look in the mutator for a left leaf variable
     * and an is left most flag. More on those later as neither are set.
     * <p>
     * We tell the mutator that we have a last item and that the action has
     * failed, by setting the fail action. Failure means we try again.
     * <p>
     * On the retry, as we descend the tree, we have the last item variable
     * set in the mutator. 
     * <p>
     * Note that we are descending the tree again. Because we are a concurrent
     * data structure, the structure of the tree may change. I'll get to that.
     * For now, let's assume that it has not changed.
     * <p>
     * If it has not changed, then we are going to encounter a pivot that has
     * our last item. When we encounter this pivot, we are going to go left.
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
     * Now we continue our descent. Eventually, we reach out chain of inner
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
     * I revisited this logic after a year and it took me a while to convince
     * myself that it was not a misunderstanding on my earlier self's part,
     * that these linked lists of otherwise empty tiers are a natural
     * occurrence.
     * <p>
     * The could be addressed by linking the inner tiers and thinking harder,
     * but that would increase the size of the project.
     */
    private final static class MergeInner
    implements Decision
    {
        /**
         * Determine if we are deleting a final leaf in a child and therefore
         * need to lock exclusive and retreive the leaf to the left.
         *
         * @param mutation The state of the current mutation.
         * @param branch The current branch.
         * @param onlyChild The value of the last item that is about to be
         * removed from a leaf tier.
         */
        private boolean lockLeft(Mutation mutation, Branch branch, Object
        onlyChild)
        {
            if (onlyChild != null && branch.getPivot() != null && !mutation.mapOfVariables.containsKey(LEFT_LEAF))
            {
                Comparable<?>[] fields = mutation.getFields(onlyChild);
                Comparable<?>[] pivotFields = mutation.getFields(branch.getPivot());
                return compare(fields, pivotFields) == 0;
            }
            return false;
        }

        /**
         * Determine if we need to merge or delete this inner tier.
         * <p>
         * We merge two branches if the child at the branch we descend can be
         * combined with either sibling. If the child can merge this method
         * returns true and a merge action is added to the parent operations.
         * <p>
         * We delete if the branch we descend has only one child. We begin to
         * mark a deletion chain. Deletion is canceled if we encounter any
         * child inner tier that has more than one child itself. We cannot
         * delete an inner tier as the result of a merge of it's children.
         * <p>
         * Additionally, if we are on a second pass after having determined
         * that we deleting the last item in a leaf tier that is an only
         * child, then we will also find and lock the leaf tier to the left of
         * the only child leaf tier. When we encounter the branch that uses
         * the item as a pivot, we'll travel to the right most leaf tier of
         * the branch to the left of the branch that uses the item as a pivot,
         * locking every level exclusively and locking the right most leaf
         * tier exclusively and noting it as the left leaf in the mutator. The
         * locks recorded in the level of the parent.
         *
         * @param mutation The state of the mutation.
         * @param levelOfParent The locks and operations of the parent.
         * @param levelOfChidl the locks and operations of child.
         * @param parent The parent tier.
         */
        public boolean test(Mutation mutation, Level levelOfParent,
                            Level levelOfChild, InnerTier parent)
        {
            // Find the child tier.

            Branch branch = parent.find(mutation.getTxn(), mutation.fields);
            InnerTier child = mutation.getInnerTier(branch.getRightKey());

            // FIXME We do not actually need to store the search value, only a
            // boolean that indicates that we are searching for an only child.
            Object search = mutation.mapOfVariables.get(SEARCH);

            // If we are on our way down to remove the last item of a leaf
            // tier that is an only child, then we need to find the leaf to
            // the left of the only child leaf tier. This means that we need
            // to detect the branch that uses the the value of the last item in
            // the only child leaf as a pivot. When we detect it we then
            // navigate each right most branch of the tier referenced by the
            // branch before it to find the leaf to the left of the only child
            // leaf. We then make note of it so we can link it around the only
            // child that is go be removed.

            if (lockLeft(mutation, branch, search))
            {
                // FIXME You need to hold these exclusive locks, so add an
                // operation that is uncancelable, but does nothing.

                int index = parent.getIndexOfTier(child.getTier().getKey()) - 1;
                InnerTier inner = parent;
                while (inner.getChildType() == INNER)
                {
                    inner = mutation.getInnerTier(inner.get(index).getRightKey());
                    levelOfParent.lockAndAdd(inner.getTier());
                    index = inner.getTier().size() - 1;
                }
                LeafTier leaf = mutation.getLeafTier(inner.get(index).getRightKey());
                levelOfParent.lockAndAdd(leaf.getTier());
                mutation.mapOfVariables.put(LEFT_LEAF, leaf);
            }


            // When we detect an inner tier with an only child, we note that
            // we have begun to descend a list of tiers with only one child.
            // Tiers with only one child are deleted rather than merged. If we
            // encounter a tier with children with siblings, we are no longer
            // deleting.

            if (child.getTier().size() == 1)
            {
                if (!mutation.mapOfVariables.containsKey(DELETING))
                {
                    mutation.mapOfVariables.put(DELETING, DELETING);
                }
                levelOfParent.listOfOperations.add(new Remove(parent, child));
                return true;
            }

            // Determine if we can merge with either sibling.

            List<InnerTier> listToMerge = new ArrayList<InnerTier>(2);

            int index = parent.getIndexOfTier(child.getTier().getKey());
            if (index != 0)
            {
                InnerTier left = mutation.getInnerTier(parent.get(index - 1).getRightKey());
                levelOfChild.lockAndAdd(left.getTier());
                levelOfChild.lockAndAdd(child.getTier());
                if (left.getTier().size() + child.getTier().size() <= mutation.getStructure().getSchema().getInnerSize())
                {
                    listToMerge.add(left);
                    listToMerge.add(child);
                }
            }

            if (index == 0)
            {
                levelOfChild.lockAndAdd(child.getTier());
            }

            if (listToMerge.isEmpty() && index != parent.getTier().size() - 1)
            {
                InnerTier right = mutation.getInnerTier(parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(right.getTier());
                if ((child.getTier().size() + right.getTier().size() - 1) == mutation.getStructure().getSchema().getInnerSize())
                {
                    listToMerge.add(child);
                    listToMerge.add(right);
                }
            }

            // Add the merge operation.

            if (listToMerge.size() != 0)
            {
                // If the parent or ancestors have only children and we are
                // creating a chain of delete operations, we have to cancel
                // those delete operations. We cannot delete an inner tier as
                // the result of a merge, we have to allow this subtree of
                // nearly empty tiers to exist. We rewind all the operations
                // above us, but we leave the top two tiers locked exclusively.

                // FIXME I'm not sure that rewind is going to remove all the
                // operations. The number here indicates that two levels are
                // supposed to be left locked exculsive, but I don't see in
                // rewind, how the operations are removed.

                if (mutation.mapOfVariables.containsKey(DELETING))
                {
                    mutation.rewind(2);
                    mutation.mapOfVariables.remove(DELETING);
                }

                levelOfParent.listOfOperations.add(new Merge(parent, listToMerge));

                return true;
            }

            // When we encounter an inner tier without an only child, then we
            // are no longer deleting. Returning false will cause the Query to
            // rewind the exclusive locks and cancel the delete operations, so
            // the delete action is reset.

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
                InnerTier left = listToMerge.get(0);
                InnerTier right = listToMerge.get(1);

                int index = parent.getIndexOfTier(right.getTier().getKey());
                Branch branch = parent.remove(index);

                right.get(0).setPivot(branch.getPivot());
                while (right.getTier().size() != 0)
                {
                    left.add(right.remove(0));
                }

                mutation.getCache().getBranchSet().remove(right.getTier());
                mutation.getStorage().getBranchStore().free(mutation.getTxn(), right.getTier());

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), parent.getTier());
                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), left.getTier());
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
                int index = parent.getIndexOfTier(child.getTier().getKey());

                parent.remove(index);
                if (parent.getTier().size() != 0)
                {
                    parent.get(0).setPivot(null);
                }

                mutation.getCache().getBranchSet().remove(child.getTier());
                mutation.getStorage().getBranchStore().free(mutation.getTxn(), child.getTier());

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), parent.getTier());
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
            Branch branch = parent.find(mutation.getTxn(), mutation.fields);
            int index = parent.getIndexOfTier(branch.getRightKey());
            LeafTier previous = null;
            LeafTier leaf = null;
            List<LeafTier> listToMerge = new ArrayList<LeafTier>();
            if (index != 0)
            {
                previous = mutation.getLeafTier(parent.get(index - 1).getRightKey());
                levelOfChild.lockAndAdd(previous.getTier());
                leaf = mutation.getLeafTier(branch.getRightKey());
                levelOfChild.lockAndAdd(leaf.getTier());
                int capacity = previous.getTier().size() + leaf.getTier().size();
                if (capacity <= mutation.getStructure().getSchema().getLeafSize() + 1)
                {
                    listToMerge.add(previous);
                    listToMerge.add(leaf);
                }
                else
                {
                    levelOfChild.unlockAndRemove(previous.getTier());
                }
            }

            if (leaf == null)
            {
                leaf = mutation.getLeafTier(branch.getRightKey());
                levelOfChild.lockAndAdd(leaf.getTier());
            }

            // TODO Do not need the parent size test, just need deleting.
            if (leaf.getTier().size() == 1 && parent.getTier().size() == 1 && mutation.mapOfVariables.containsKey(DELETING))
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
            else if (listToMerge.isEmpty() && index != parent.getTier().size() - 1)
            {
                LeafTier next = mutation.getLeafTier(parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(next.getTier());
                int capacity = next.getTier().size() + leaf.getTier().size();
                if (capacity <= mutation.getStructure().getSchema().getLeafSize() + 1)
                {
                    listToMerge.add(leaf);
                    listToMerge.add(next);
                }
                else
                {
                    levelOfChild.unlockAndRemove(next.getTier());
                }
            }

            if (listToMerge.isEmpty())
            {
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
                LeafTier left = listToMerge.get(0);
                LeafTier right = listToMerge.get(1);
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
                    Iterator<Object> objects = leaf.getTier().iterator();
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
                            if (mutation.deletable.deletable(mutation.getStructure().getObjectKey(candidate)))
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
                            mutation.getCache().getLeafSet().dirty(mutation.getTxn(), current.getTier());
                            mutation.mapOfVariables.put(RESULT, candidate);
                            break SEARCH;
                        }
                    }
                    current = current.getNextAndLock(mutation, levelOfLeaf);
                }
                while (current != null && compare(mutation.fields, mutation.getFields(current.get(0))) == 0);

                if (mutation.mapOfVariables.containsKey(RESULT) && count == found && current.getTier().size() == mutation.getStructure().getSchema().getLeafSize() - 1 && compare(mutation.fields, mutation.getFields(current.get(current.getTier().size() - 1))) == 0)
                {
                    for (;;)
                    {
                        LeafTier subsequent = current.getNextAndLock(mutation, levelOfLeaf);
                        if (subsequent == null || compare(mutation.fields, mutation.getFields(subsequent.get(0))) != 0)
                        {
                            break;
                        }
                        current.addBucket(subsequent.remove(0));
                        if (subsequent.getTier().size() == 0)
                        {
                            current.setNextLeafKey(subsequent.getNextLeafKey());
                            mutation.getCache().getLeafSet().remove(subsequent.getTier());
                            mutation.getStorage().getLeafStore().free(mutation.getTxn(), subsequent.getTier());
                        }
                        else
                        {
                            mutation.getCache().getLeafSet().dirty(mutation.getTxn(), subsequent.getTier());
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
                parent.remove(parent.getIndexOfTier(right.getTier().getKey()));

                while (right.getTier().size() != 0)
                {
                    left.addBucket(right.remove(0));
                }
                // FIXME Get last leaf. 
                left.setNextLeafKey(right.getNextLeafKey());

                mutation.getCache().getLeafSet().remove(right.getTier());
                mutation.getStorage().getLeafStore().free(mutation.getTxn(), right.getTier());

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), parent.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), left.getTier());
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
                parent.remove(parent.getIndexOfTier(leaf.getTier().getKey()));

                left.setNextLeafKey(leaf.getNextLeafKey());

                mutation.getCache().getLeafSet().remove(leaf.getTier());
                mutation.getStorage().getLeafStore().free(mutation.getTxn(), leaf.getTier());

                mutation.getCache().getBranchSet().dirty(mutation.getTxn(), parent.getTier());
                mutation.getCache().getLeafSet().dirty(mutation.getTxn(), left.getTier());

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

        public final Map<Object, Tier<?>> mapOfLockedTiers = new HashMap<Object, Tier<?>>();

        public final LinkedList<Operation> listOfOperations = new LinkedList<Operation>();

        public Level(boolean exclusive)
        {
            this.getSync = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
        }

        public void lockAndAdd(Tier<?> tier)
        {
            lock_(tier);
            add_(tier);
        }
        
        public void unlockAndRemove(Tier<?> tier)
        {
            assert mapOfLockedTiers.containsKey(tier.getKey());
            
            mapOfLockedTiers.remove(tier.getKey());
            unlock_(tier);
        }

        public void add_(Tier<?> tier)
        {
            mapOfLockedTiers.put(tier.getKey(), tier);
        }

        public void lock_(Tier<?> tier)
        {
            getSync.getSync(tier.getReadWriteLock()).lock();
        }

        public void unlock_(Tier<?> tier)
        {
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }

        public void release()
        {
            Iterator<Tier<?>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?> tier = lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
        }

        public void releaseAndClear()
        {
            Iterator<Tier<?>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?> tier = lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
            mapOfLockedTiers.clear();
        }

        private void exclusive()
        {
            Iterator<Tier<?>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?> tier = lockedTiers.next();
                tier.getReadWriteLock().writeLock().lock();
            }
            getSync = new WriteLockExtractor();
        }

        public void downgrade()
        {
            if (getSync.isExeclusive())
            {
                Iterator<Tier<?>> lockedTiers = mapOfLockedTiers.values().iterator();
                while (lockedTiers.hasNext())
                {
                    Tier<?> tier = lockedTiers.next();
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
        private final Navigator navigator;
        
        private final TierCache<Object> cache;

        private final Object rootKey;

        private final Strata strata;

        public Query(Object txn, Strata strata, TierCache<Object> cache)
        {
            this.navigator = new Navigator(strata.structure, strata.storage, txn);
            this.cache = cache;
            this.strata = strata;
            this.rootKey = strata.rootKey;
        }

        public Strata getStrata()
        {
            return strata;
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
            cache.begin();

            mutation.listOfLevels.add(new Level(false));

            InnerTier parent = getRoot();
            Level levelOfParent = new Level(false);
            levelOfParent.lockAndAdd(parent.getTier());
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
                    Branch branch = parent.find(navigator, mutation.fields);
                    InnerTier child = navigator.getInnerTier(branch.getRightKey());
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
                    Level level = levels.previous();
                    ListIterator<Operation> operations = level.listOfOperations.listIterator(level.listOfOperations.size());
                    while (operations.hasPrevious())
                    {
                        Operation operation = operations.previous();
                        operation.operate(mutation);
                    }
                }

                cache.end(navigator.getTxn());
            }

            ListIterator<Level> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
            while (levels.hasPrevious())
            {
                Level level = levels.previous();
                level.releaseAndClear();
            }

            return mutation.mapOfVariables.get(RESULT);
        }

        // FIXME Rename add.
        // FIXME Key of object?
        public void insert(Object keyOfObject)
        {
            CoreRecord record = new CoreRecord();
            strata.structure.getSchema().getFieldExtractor().extract(navigator.getTxn(), keyOfObject, record);
            Object bucket = strata.structure.newBucket(record.getComparables(), keyOfObject);
            Mutation mutation = new Mutation(navigator, cache, bucket, record.getComparables(), null);
            generalized(mutation, new SplitRoot(), new SplitInner(), new InnerNever(), new LeafInsert());
        }

        public Object remove(Object keyOfObject)
        {
            CoreRecord record = new CoreRecord();
            strata.structure.getSchema().getFieldExtractor().extract(navigator.getTxn(), keyOfObject, record);
            return remove(record.getComparables(), ANY);
        }

        // TODO Where do I actually use deletable? Makes sense, though. A
        // condition to choose which to delete.
        public Object remove(Comparable<?>[] fields, Deletable deletable)
        {
            Mutation mutation = new Mutation(navigator, cache, null, fields, deletable);
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

        public Cursor<Object> find(Object object)
        {
            CoreRecord record = new CoreRecord();
            strata.structure.getSchema().getFieldExtractor().extract(navigator.getTxn(), object, record);
            return find(record.getComparables());
        }

        /**
         * Java eliminated a modicum of type safety with this implementation
         * of comparable. Now that Java has generics, many Javans have decided
         * too, that Java no longer needs arrays. This method used to take
         * an array of comparables. Since I have to cast the objects anyway,
         * I've removed event the type checking for the comparable. It is
         * an expression of the frustration I feel for this language that
         * wants to become a language that does many things poorly and nothing
         * well. An array of Comparable<?> is confusing. The typing is missing.
         * I'm going down to object. The moment you call this method with
         * incorrect comprable parameters, is the moment that it fails, so
         * test.
         * 
         * @param fields A partial or full set of fields to compare to the
         * fields extracted from this strata.
         * @return
         */
        // Here is where I get the power of not using comparator.
        public Cursor<Object> find(Comparable<?>... fields)
        {
            Lock previous = new ReentrantLock();
            previous.lock();
            InnerTier inner = getRoot();
            for (;;)
            {
                inner.getTier().getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = inner.getTier().getReadWriteLock().readLock();
                Branch branch = inner.find(navigator, fields);
                if (inner.getChildType() == LEAF)
                {
                    LeafTier leaf = navigator.getLeafTier(branch.getRightKey());
                    leaf.getTier().getReadWriteLock().readLock().lock();
                    previous.unlock();
                    return leaf.find(navigator, fields);
                }
                inner = navigator.getInnerTier(branch.getRightKey());
            }
        }

        public Cursor<Object> first()
        {
            Branch branch = null;
            InnerTier inner = getRoot();
            Lock previous = new ReentrantLock();
            previous.lock();
            for (;;)
            {
                inner.getTier().getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = inner.getTier().getReadWriteLock().readLock();
                branch = inner.getTier().get(0);
                if (inner.getChildType() == LEAF)
                {
                    break;
                }
                inner = navigator.getInnerTier(branch.getRightKey());
            }
            LeafTier leaf = navigator.getLeafTier(branch.getRightKey());
            leaf.getTier().getReadWriteLock().readLock().lock();
            previous.unlock();
            return new Cursor<Object>(navigator, leaf, 0);
        }

        public Cursor<Object> last_UNIMPLEMENTED()
        {
            Branch branch = null;
            InnerTier inner = getRoot();
            Lock previous = new ReentrantLock();
            previous.lock();
            for (;;)
            {
                inner.getTier().getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = inner.getTier().getReadWriteLock().readLock();
                branch = inner.getTier().get(inner.getTier().size());
                if (inner.getChildType() == LEAF)
                {
                    break;
                }
                inner = navigator.getInnerTier(branch.getRightKey());
            }
            LeafTier leaf = navigator.getLeafTier(branch.getRightKey());
            leaf.getTier().getReadWriteLock().readLock().lock();
            previous.unlock();
            return new Cursor<Object>(navigator, leaf, leaf.getTier().size());
        }

        public void flush()
        {
            cache.flush(navigator.getTxn());
        }

        private void destroy(InnerTier inner)
        {
            if (inner.getChildType() == INNER)
            {
                Iterator<Branch> branches = inner.listIterator();
                while (branches.hasNext())
                {
                    Branch branch = branches.next();
                    destroy(navigator.getInnerTier(branch.getRightKey()));
                }
            }
            else
            {
                Iterator<Branch> branches = inner.listIterator();
                while (branches.hasNext())
                {
                    Branch branch = branches.next();
                    LeafTier leaf = navigator.getLeafTier(branch.getRightKey());
                    navigator.getStorage().getLeafStore().free(navigator.getTxn(), leaf.getTier());
                }
            }
            navigator.getStorage().getBranchStore().free(navigator.getTxn(), inner.getTier());
        }

        public void destroy()
        {
            // FIXME Get the write mutex.
            synchronized (this)
            {
                if (navigator.getStorage().getBranchStore().getIdentifier().isKeyNull(getRoot()))
                {
                    return;
                }
            }
            destroy(getRoot());
        }

        public void copacetic()
        {
            // FIXME Lock.
            Comparator<Object> comparator = new CopaceticComparator(navigator, strata.structure);
            getRoot().copacetic(navigator, new Copacetic(comparator));
            Cursor<Object> cursor = first();
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
            return navigator.getInnerTier(rootKey);
        }
    }

    public final static class Cursor<T>
    {
        private final Navigator navigator;
        
        private int index;

        private LeafTier leaf;

        private boolean released;

        public Cursor(Navigator navigator, LeafTier leaf, int index)
        {
            this.navigator = navigator;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isForward()
        {
            return true;
        }

        public Cursor<T> newCursor()
        {
            return new Cursor<T>(navigator, leaf, index);
        }

        public boolean hasNext()
        {
            return index < leaf.getTier().size() || !navigator.getStorage().getLeafStore().getIdentifier().isKeyNull(leaf.getNextLeafKey());
        }

        public Object next()
        {
            if (released)
            {
                throw new IllegalStateException();
            }
            if (index == leaf.getTier().size())
            {
                if (navigator.getStorage().getLeafStore().getIdentifier().isKeyNull(leaf.getNextLeafKey()))
                {
                    throw new IllegalStateException();
                }
                LeafTier next = navigator.getLeafTier(leaf.getNextLeafKey());
                next.getTier().getReadWriteLock().readLock().lock();
                leaf.getTier().getReadWriteLock().readLock().unlock();
                leaf = next;
                index = 0;
            }
            Object object = navigator.getStructure().getObjectKey(leaf.get(index++));
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
                leaf.getTier().getReadWriteLock().readLock().unlock();
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
