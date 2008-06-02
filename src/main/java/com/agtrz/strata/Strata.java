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

    public Strata()
    {
        this(new Schema(), null, new HashMap<Object, Tier>(), true);
    }

    private Strata(Schema creator, Object txn, Map<Object, Tier> mapOfDirtyTiers, boolean flush)
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
        
        if (flush)
        {
            new Query(txn, this, mapOfDirtyTiers).flush();
        }
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

        private int size;

        private boolean cacheFields;

        private int maxDirtyTiers;

        public Schema()
        {
            this.storageSchema = new ArrayListStorage.Schema();
            this.extractor = new ComparableExtractor();
            this.size = 5;
            this.cacheFields = false;
            this.maxDirtyTiers = 0;
        }

        public Schema(Schema creator)
        {
            this.storageSchema = creator.storageSchema;
            this.extractor = creator.extractor;
            this.size = creator.size;
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
            Strata strata = new Strata(this, txn, mapOfDirtyTiers, false);
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

        public void setSize(int size)
        {
            this.size = size;
        }

        public int getSize()
        {
            return size;
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
            this.listOfObjects = new ArrayList<Object>(structure.getSchema().getSize());
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
            if (getSize() == structure.getSchema().getSize())
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

            if (getSize() > structure.getSchema().getSize())
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
            if (!structure.getStorage().isKeyNull(getNextLeafKey()) && comparator.compare(get(getSize() - 1), getNext(txn).get(0)) == 0 && structure.getSchema().getSize() != getSize() && comparator.compare(get(0), get(getSize() - 1)) != 0)
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
            this.listOfBranches = new ArrayList<Branch>(structure.getSchema().getSize() + 1);
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
            return listOfBranches.size() - 1;
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

            if (getSize() > structure.getSchema().getSize())
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
            return mutation.structure.getSchema().getSize() == root.getSize();
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
                int partition = (root.getSize() + 1) / 2;
                int fullSize = root.getSize() + 1;
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
            if (child.getSize() == mutation.structure.getSchema().getSize())
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
                int partition = (child.getSize() + 1) / 2;

                while (partition <= child.getSize())
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
            if (leaf.getSize() == mutation.structure.getSchema().getSize())
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

    private final static class DeleteRoot
    implements RootDecision
    {
        public boolean test(Mutation mutation, Level levelOfRoot, InnerTier root)
        {
            if (root.getChildType() == INNER && root.getSize() == 1)
            {
                InnerTier first = (InnerTier) root.getTier(mutation.txn, root.get(0).getRightKey());
                InnerTier second = (InnerTier) root.getTier(mutation.txn, root.get(1).getRightKey());
                return first.getSize() + second.getSize() == mutation.structure.getSchema().getSize();
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
                while (child.getSize() != -1)
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

    private final static class MergeInner
    implements Decision
    {
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

        public boolean test(Mutation mutation, Level levelOfParent, Level levelOfChild, InnerTier parent)
        {
            Branch branch = parent.find(mutation.txn, mutation.fields);
            InnerTier child = (InnerTier) parent.getTier(mutation.txn, branch.getRightKey());

            Object search = mutation.mapOfVariables.get(SEARCH);

            if (lockLeft(mutation, branch, search))
            {
                Storage storage = mutation.structure.getStorage();
                int index = parent.getIndexOfTier(child.getKey()) - 1;
                InnerTier inner = parent;
                while (inner.getChildType() == INNER)
                {
                    inner = storage.getInnerTier(mutation.structure, mutation.txn, inner.get(index).getRightKey());
                    levelOfParent.lockAndAdd(inner);
                    index = inner.getSize();
                }
                LeafTier leaf = storage.getLeafTier(mutation.structure, mutation.txn, inner.get(index).getRightKey());
                levelOfParent.lockAndAdd(leaf);
                mutation.mapOfVariables.put(LEFT_LEAF, leaf);
            }

            if (child.getSize() == 0)
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
                if (left.getSize() + child.getSize() <= mutation.structure.getSchema().getSize())
                {
                    listToMerge.add(left);
                    listToMerge.add(child);
                }
            }

            if (index == 0)
            {
                levelOfChild.lockAndAdd(child);
            }

            if (listToMerge.isEmpty() && index != parent.getSize())
            {
                InnerTier right = (InnerTier) parent.getTier(mutation.txn, parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(right);
                if (child.getSize() + right.getSize() <= mutation.structure.getSchema().getSize())
                {
                    listToMerge.add(child);
                    listToMerge.add(right);
                }
            }

            if (!listToMerge.isEmpty())
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
                while (right.getSize() != -1)
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
                parent.get(0).setPivot(null);

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
                levelOfChild.lock(previous);
                leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
                levelOfChild.lock(leaf);
                int capacity = previous.getSize() + leaf.getSize();
                if (capacity <= mutation.structure.getSchema().getSize() + 1)
                {
                    listToMerge.add(previous);
                    listToMerge.add(leaf);
                }
                else
                {
                    levelOfChild.release(previous);
                }
            }

            if (leaf == null)
            {
                leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
                levelOfChild.lock(leaf);
            }

            if (leaf.getSize() == 1 && parent.getSize() == 0 && mutation.mapOfVariables.containsKey(DELETING))
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
            else if (listToMerge.isEmpty() && index != parent.getSize())
            {

                LeafTier next = (LeafTier) parent.getTier(mutation.txn, parent.get(index + 1).getRightKey());
                levelOfChild.lock(next);
                int capacity = next.getSize() + leaf.getSize();
                if (capacity <= mutation.structure.getSchema().getSize() + 1)
                {
                    listToMerge.add(leaf);
                    listToMerge.add(next);
                }
                else
                {
                    levelOfChild.release(next);
                }
            }
            if (listToMerge.isEmpty())
            {
                if (leaf == null)
                {
                    leaf = (LeafTier) parent.getTier(mutation.txn, branch.getRightKey());
                    levelOfChild.lock(leaf);
                }
                levelOfChild.add(leaf);
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
                levelOfChild.add(left);
                levelOfChild.add(right);
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

                if (mutation.mapOfVariables.containsKey(RESULT) && count == found && current.getSize() == mutation.structure.getSchema().getSize() - 1 && compare(mutation.fields, mutation.getFields(current.get(current.getSize() - 1))) == 0)
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
        {
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

        public final LinkedList<Tier> listOfLockedTiers = new LinkedList<Tier>();

        public final LinkedList<Operation> listOfOperations = new LinkedList<Operation>();

        public Level(boolean exclusive)
        {
            this.getSync = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
        }

        public ReadWriteLock getReadWriteLock()
        {
            return (ReadWriteLock) listOfLockedTiers.getFirst();
        }

        public void lockAndAdd(Tier tier)
        {
            lock(tier);
            add(tier);
        }

        public void add(Tier tier)
        {
            listOfLockedTiers.add(tier);
        }

        public void lock(Tier tier)
        {
            getSync.getSync(tier.getReadWriteLock()).lock();
        }

        public void release(Tier tier)
        {
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }

        public void release()
        {
            Iterator<Tier> lockedTiers = listOfLockedTiers.iterator();
            while (lockedTiers.hasNext())
            {
                Tier tier = (Tier) lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
        }

        public void releaseAndClear()
        {
            Iterator<Tier> lockedTiers = listOfLockedTiers.iterator();
            while (lockedTiers.hasNext())
            {
                Tier tier = (Tier) lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
            listOfLockedTiers.clear();
        }

        private void exclusive()
        {
            Iterator<Tier> lockedTiers = listOfLockedTiers.iterator();
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
                Iterator<Tier> lockedTiers = listOfLockedTiers.iterator();
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
                levelOfChild.listOfLockedTiers.clear();
                exclusive();
                levelOfChild.exclusive();
                return true;
            }
            else if (!levelOfChild.getSync.isExeclusive())
            {
                levelOfChild.release();
                levelOfChild.listOfLockedTiers.clear();
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

        private Object generalized(Mutation mutation, RootDecision initial, Decision subsequent, Decision swap, Decision penultimate)
        {
            if (mapOfDirtyTiers.size() == 0)
            {
                strata.writeMutex.lock();
            }

            Storage storage = strata.structure.getStorage();
            synchronized (this)
            {
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

                    // strata.rootKey = root.getKey();

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

        public Cursor find(Comparable<?>[] fields)
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
