/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Strata
{
    
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

    public interface Record
    {
        public void fields(Comparable<?>... fields);
    }
    
    private final static class CoreRecord
    implements Record
    {
        private Comparable<?>[] fields;

        public void fields(Comparable<?>... fields)
        {
            this.fields = fields;
        }
        
        public Comparable<?>[] getFields()
        {
            return fields;
        }
    }
    
    public interface Extractor<T, X>
    {
        public void extract(X txn, T object, Record record);
    }
    
    public interface TierBuilder<H, T>
    {
        public void setHeader(H header);
        
        public void append(T object);
    }
    
    public interface Cooper<T, B, X>
    {
        public B newBucket(X txn, Extractor<T, X> extractor, T object);

        public B newBucket(Comparable<?>[] fields, T object);

        public T getObject(B bucket);

        public Comparable<?>[] getFields(X txn, Extractor<T, X> extractor, B bucket);
        
        public Cursor<T> wrap(Cursor<B> cursor);

        public boolean getCacheFields();
    }
    
    public final static class Bucket<T>
    {
        public final Comparable<?>[] fields;

        public final T object;

        public Bucket(Comparable<?>[] fields, T object)
        {
            this.fields = fields;
            this.object = object;
        }
    }

    public static class BucketCooper<T, X>
    implements Cooper<T, Bucket<T>, X>, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public Bucket<T> newBucket(X txn, Extractor<T, X> extractor, T object)
        {
            CoreRecord record = new CoreRecord();
            extractor.extract(txn, object, record);
            return new Bucket<T>(record.getFields(), object);
        }

        public Bucket<T> newBucket(Comparable<?>[] fields, T object)
        {
            return new Bucket<T>(fields, object);
        }

        public Comparable<?>[] getFields(X txn, Extractor<T, X> extractor, Bucket<T> bucket)
        {
            return bucket.fields;
        }

        public T getObject(Bucket<T> bucket)
        {
            return bucket.object;
        }
        
        public Cursor<T> wrap(Cursor<Bucket<T>> cursor)
        {
            return new BucketCursor<T, Bucket<T>, X>(cursor);
        }

        public boolean getCacheFields()
        {
            return true;
        }
    }

    public final static class LookupCooper<T, X>
    implements Cooper<T, T, X>, Serializable
    {
        private final static long serialVersionUID = 20070402L;

        public T newBucket(X txn, Extractor<T, X> extract, T object)
        {
            return object;
        }

        public T newBucket(Comparable<?>[] fields, T object)
        {
            return object;
        }

        public Comparable<?>[] getFields(X txn, Extractor<T, X> extractor, T object)
        {
            CoreRecord record = new CoreRecord();
            extractor.extract(txn, object, record);
            return record.getFields();
        }

        public T getObject(T object)
        {
            return object;
        }
        
        public Cursor<T> wrap(Cursor<T> cursor)
        {
            return cursor;
        }

        public boolean getCacheFields()
        {
            return false;
        }
    }

    public final static class BucketComparable<T, B, X>
    implements Comparable<B>
    {
        private Cooper<T, B, X> cooper;

        private Extractor<T, X> extractor;

        private X txn;

        private Comparable<?>[] fields;

        public int compareTo(B bucket)
        {
            return compare(fields, cooper.getFields(txn, extractor, bucket));
        }
    }

    public abstract static class Tier<B, A>
    extends ArrayList<B>
    {
        private final ReadWriteLock readWriteLock;

        private A address;
        
        public Tier()
        {
            this.readWriteLock = new ReentrantReadWriteLock();
        }
        
        public ReadWriteLock getReadWriteLock()
        {
            return readWriteLock;
        }

        public A getAddress()
        {
            return address;
        }
        
        public void setAddress(A address)
        {
            this.address = address;
        }
        
        @Override
        public boolean equals(Object o)
        {
            return o == this;
        }
        
        @Override
        public int hashCode()
        {
            return System.identityHashCode(this);
        }
        
//        public abstract <B2, X> void write(Storage<B2, A, X> storage);
    }

    public final static class LeafTier<B, A>
    extends Tier<B, A>
    {
        private static final long serialVersionUID = 1L;

        private A next;
        
        public ReadWriteLock getReadWriteLock()
        {
            return null;
        }

        public Cursor<B> find(Comparable<B> comparable)
        {
            return null;
        }
        
        public <X> void link(Mutation<B, A, X> mutation, LeafTier<B, A> leaf)
        {
            
        }
        
        public <X> LeafTier<B, A> getNextAndLock(Mutation<B, A, X> mutation, Level<B, A, X> leafLevel)
        {
            return null;
        }
        
        public <X> LeafTier<B, A> append(Mutation<B, A, X> mutation, Level<B, A, X> leafLevel)
        {
            return null;
        }

        public <X> LeafTier<B, A> getNext(Mutation<B, A, X> mutation)
        {
            return null;
        }
        
        public A getNext()
        {
            return next;
        }
        
        public void setNext(A next)
        {
            this.next = next;
        }

//        public <B2, X> void write(Storage<B2, A, X> storage, X txn)
//        {
//            Collection<B> collection = this;
//            storage.getLeafStore().write(txn, getAddress(), getNext(), collection);
//        }
    }

    public enum ChildType
    {
        INNER, LEAF
    }

    public final static class Branch<T, A>
    {
        private final A key;

        private T pivot;

        public Branch(T pivot, A address)
        {
            this.key = address;
            this.pivot = pivot;
        }

        public A getRightKey()
        {
            return key;
        }

        public T getPivot()
        {
            return pivot;
        }

        public void setPivot(T pivot)
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

    public static class InnerTier<B, A>
    extends Tier<Branch<B, A>, A>
    {
        private static final long serialVersionUID = 1L;

        private ChildType childType;

        public ChildType getChildType()
        {
            return childType;
        }
        
        public int getIndex(A address)
        {
            int index = 0;
            Iterator<Branch<B, A>> branches = iterator();
            while (branches.hasNext())
            {
                Branch<B, A> branch = branches.next();
                if (branch.getRightKey().equals(address))
                {
                    return index;
                }
                index++;
            }
            return -1;
        }
        
        public void setChildType(ChildType childType)
        {
            this.childType = childType;
        }

        public Branch<B, A> find(Comparable<B> comparable)
        {
            return null;
        }
    }
    
    public static final class RawTier<T, A, H>
    {
        private H header;
        
        private final List<T> objects;
        
        private final List<A> addresses;
        
        public RawTier()
        {
            this.objects = new ArrayList<T>();
            this.addresses = new ArrayList<A>();
        }

        public void addObject(T object)
        {
            objects.add(object);
        }
        
        public List<T> getObjects()
        {
            return objects;
        }
        
        public void addAddress(A address)
        {
            addresses.add(address);
        }
        
        public List<A> getAddresses()
        {
            return addresses;
        }
        
        public void setHeader(H header)
        {
            this.header = header;
        }
        
        public H getHeader()
        {
            return header;
        }
    }
    
    public interface HeaderIO<H>
    {
        public <T, A> void write(RawTier<T, A, H> tier, Object object);
        
        public <T, A> void read(RawTier<T, A, H> tier, Object object);
    }
    
    public interface LeafStore<T, A, X>
    {
        public A allocate(X txn, int size);
        
        public <B> LeafTier<B, A> load(X txn, A address, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
        
        public <B> void write(X txn, LeafTier<B, A> leaf, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
        
        public void free(X txn, A address);
    }
    
    public interface InnerStore<T, A, X>
    {
        public A allocate(X txn, int size);
        
        public <B> InnerTier<B, A> load(X txn, A address, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
        
        public <B> void write(X txn, InnerTier<B, A> inner, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
        
        public void free(X txn, A address);
    }
    
    public interface Storage<T, A, X>
    {
        public InnerStore<T, A, X> getInnerStore();
        
        public LeafStore<T, A, X> getLeafStore();
        
        public void commit(X txn);
    }
    
    public interface Allocator<B, A, X>
    {
        public A allocate(X txn, InnerTier<B, A> inner);
        
        public A allocate(X txn, LeafTier<B, A> leaf);
        
        public boolean isNull(A address);
        
        public A getNull();
    }
    
    private final static class NullAllocator<B>
    implements Allocator<B, Object, Object>
    {
        public Object allocate(Object txn, InnerTier<B, Object> inner)
        {
            return null;
        }
        
        public Object allocate(Object txn, LeafTier<B, Object> leaf)
        {
            return null;
        }
        
        public boolean isNull(Object address)
        {
            return address == null;
        }
        
        public Object getNull()
        {
            return null;
        }
    }

    public static class Navigator<B, A, X>
    {
        private final X txn;
        
        private final Allocator<B, A, X> allocator;
        
        private TierPool<B, A, X> pool;

        public Navigator(X txn, Allocator<B, A, X> allocator, TierPool<B, A, X> pool)
        {
            this.txn = txn;
            this.allocator = allocator;
            this.pool = pool;
        }

        public Navigator(Navigator<B, A, X> navigator)
        {
            this.txn = navigator.txn;
            this.allocator = navigator.allocator;
        }
        
        public X getTxn()
        {
            return txn;
        }
         
        public TierPool<B, A, X> getPool()
        {
            return pool;
        }
       
        public Allocator<B, A, X> getAllocator()
        {
            return allocator;
        }

        public LeafTier<B, A> getLeafTier(A address)
        {
            return pool.getLeafTier(txn, address);
        }

        public InnerTier<B, A> getInnerTier(A address)
        {
            return pool.getInnerTier(txn, address);
        }
    }

    public interface Cursor<T>
    extends Iterator<T>
    {

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
    
    private final static class Level<B, A, X>
    {
        public LockExtractor getSync;

        public final Map<Object, Tier<?, A>> mapOfLockedTiers = new HashMap<Object, Tier<?, A>>();

        public final LinkedList<Operation<B, A, X>> listOfOperations = new LinkedList<Operation<B, A, X>>();

        public Level(boolean exclusive)
        {
            this.getSync = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
        }

        public void lockAndAdd(Tier<?, A> tier)
        {
            lock_(tier);
            add_(tier);
        }
        
        public void unlockAndRemove(Tier<?, A> tier)
        {
            assert mapOfLockedTiers.containsKey(tier.getAddress());
            
            mapOfLockedTiers.remove(tier.getAddress());
            unlock_(tier);
        }

        public void add_(Tier<?, A> tier)
        {
            mapOfLockedTiers.put(tier.getAddress(), tier);
        }

        public void lock_(Tier<?, A> tier)
        {
            getSync.getSync(tier.getReadWriteLock()).lock();
        }

        public void unlock_(Tier<?, A> tier)
        {
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }

        public void release()
        {
            Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?, A> tier = lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
        }

        public void releaseAndClear()
        {
            Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?, A> tier = lockedTiers.next();
                getSync.getSync(tier.getReadWriteLock()).unlock();
            }
            mapOfLockedTiers.clear();
        }

        private void exclusive()
        {
            Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?, A> tier = lockedTiers.next();
                tier.getReadWriteLock().writeLock().lock();
            }
            getSync = new WriteLockExtractor();
        }

        public void downgrade()
        {
            if (getSync.isExeclusive())
            {
                Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
                while (lockedTiers.hasNext())
                {
                    Tier<?, A> tier = lockedTiers.next();
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

        public boolean upgrade(Level<B, A, X> levelOfChild)
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

    interface AutoCommit<X>
    {
        public void autoCommit(X txn);
    }
    
    interface TierSet<B, A, X>
    {
        
        /**
         * Record a tier as dirty in the tier cache.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(X txn, Tier<B, A> tier);
        
        /**
         * Remove a dirty tier from the tier cache.
         * 
         * @param tier The tier to remove.
         */
        public void remove(Tier<B, A> tier);
        
        public void write(X txn);
        
        public int size();
    }

    /**
     * A strategy for both caching dirty tiers in order to writing them out to
     * storage in a batch as well as for locking the Strata for exclusive
     * insert and delete.
     */
    public interface TierWriter<B, A, X>
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
         * Notify the tier cache that an insert or delete is about to begin
         * so that the tier cache can acquire locks if necessary.
         */
        public void begin();

        /**
         * Record a tier as dirty in the tier cache.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(X txn, InnerTier<B, A> tier);
        
        /**
         * Remove a dirty tier from the tier cache.
         * 
         * @param tier The tier to remove.
         */
        public void remove(InnerTier<B, A> tier);
        
        /**
         * Record a tier as dirty in the tier cache.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         * @param tier The dirty tier.
         */
        public void dirty(X txn, LeafTier<B, A> tier);
        
        /**
         * Remove a dirty tier from the tier cache.
         * 
         * @param tier The tier to remove.
         */
        public void remove(LeafTier<B, A> tier);
        
        /**
         * Notify the tier cache that an insert or delete has completed and
         * so that the tier cache can determine if the cache should be flushed. 
         * If the tier cache is flushed and the auto commit property is true,
         * the tier cache will call the commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void end(X txn);
        
        /**
         * Flush any dirty pages in the tier cache and empty the tier cache.
         * If the auto commit property is true, the tier cache will call the
         * commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void flush(X txn);
        
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
        public TierWriter<B, A, X> newTierWriter();
    } 
    
    public static class EmptyTierSet<A, H, B, X>
    implements TierSet<B, A, X>
    {
        public EmptyTierSet()
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
        public void dirty(X txn, Tier<B, A> tier)
        {
        }
        
        /**
         * For the empty tier cache, this method does nothing.
         * 
         * @param tier The tier to remove.
         */
        public void remove(Tier<B, A> tier)
        {
        }
        
        public void write(X txn)
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
    public static class EmptyTierCache<B, A, X>
    implements TierWriter<B, A, X>, AutoCommit<X>
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
        
        public EmptyTierCache()
        {
            this(new ReentrantLock(), true);
        }
        
        /**
         * Create an empty tier cache with 
         */
        protected EmptyTierCache(Lock lock, boolean autoCommit)
        {
            this.lock = lock;
            this.autoCommit = autoCommit;
        }
        
        public void autoCommit(X txn)
        {
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

        public void dirty(X txn, InnerTier<B, A> inner)
        {
        }

        public void remove(InnerTier<B, A> inner)
        {
        }

        public void dirty(X txn, LeafTier<B, A> leaf)
        {
        }

        public void remove(LeafTier<B, A> leaf)
        {
        }
        
        /**
         * A noop implementation of storage synchronization called after an
         * insert or delete of an object from the strata.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void end(X txn)
        {
        }
          
        /**
         * Since the cache is always empty, this method merely calls the
         * commit method of the storage strategy.
         *
         * @param storage The storage strategy.
         * @param txn A storage specific state object.
         */
        public void flush(X txn)
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
        public TierWriter<B, A, X> newTierWriter()
        {
            return new EmptyTierCache<B, A, X>(lock, autoCommit);
        }
    }
    
    /**
     * Keeps a synchronized map of dirty tiers with a maximum size at which
     * the tiers are written to file and flushed. Used as the base class of
     * both the per query and per strata implementations of the tier cache.
     */
    static class AbstractTierCache<T, B, A, X>
    extends EmptyTierCache<B, A, X>
    {
        private final Map<A, LeafTier<B, A>> leafTiers;
        
        private final Map<A, InnerTier<B, A>> innerTiers;
        
        private final Storage<T, A, X> storage;
        
        protected final Cooper<T, B, X> cooper;
        
        protected final Extractor<T, X> extractor;

        protected final Object mutex;
        
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
        public AbstractTierCache(Storage<T, A, X> storage,
                                 Cooper<T, B, X> cooper,
                                 Extractor<T, X> extractor,
                                 Lock lock,
                                 Object mutex,
                                 int max,
                                 boolean autoCommit)
        {
            super(lock, autoCommit);
            this.storage = storage;
            this.cooper = cooper;
            this.extractor = extractor;
            this.mutex = mutex;
            this.max = max;
            this.leafTiers = new HashMap<A, LeafTier<B,A>>();
            this.innerTiers = new HashMap<A, InnerTier<B,A>>();
        }

        public void autoCommit(X txn)
        {
            if (isAutoCommit())
            {
                storage.commit(txn);
            }
        }
        
        public Storage<T, A, X> getStorage()
        {
            return storage;
        }
        
        public int size()
        {
            return innerTiers.size() + leafTiers.size();
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
        protected void save(X txn, boolean force)
        {
            synchronized (mutex)
            {
                if (force || innerTiers.size() + leafTiers.size() >= max)
                {
                    for (InnerTier<B, A> inner : innerTiers.values())
                    {
                        storage.getInnerStore().write(txn, inner, cooper, extractor);
                    }
                    innerTiers.clear();
                    for (LeafTier<B, A> leaf : leafTiers.values())
                    {
                        storage.getLeafStore().write(txn, leaf, cooper, extractor);
                    }
                    leafTiers.clear();
                    if (isAutoCommit())
                    {
                        getStorage().commit(txn);
                    }
                }
            }
        }
        
        @Override
        public void dirty(X txn, Strata.InnerTier<B,A> inner)
        {
            synchronized (mutex)
            {
                innerTiers.put(inner.getAddress(), inner);
            }
        }
        
        @Override
        public void remove(InnerTier<B, A> inner)
        {
            synchronized (mutex)
            {
                innerTiers.remove(inner);
            }
        }
        
        @Override
        public void dirty(X txn, Strata.LeafTier<B,A> leaf)
        {
            synchronized (mutex)
            {
                leafTiers.put(leaf.getAddress(), leaf);
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
        public void flush(X txn)
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
    public static class PerQueryTierCache<T, B, A, X>
    extends AbstractTierCache<T, B, A, X>
    {
        /**
         * Create a per query tier cache.
         *
         * @param lock An exclusive lock on the Strata.
         * @param max The dirty tier cache size that when reached, will cause
         * the cache to empty and the tiers to be written.
         */
        public PerQueryTierCache(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor, int max)
        {
            this(storage, cooper, extractor, new ReentrantLock(), max, true);
        }
        
        private PerQueryTierCache(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor, Lock lock, int max, boolean autoCommit)
        {
            super(storage, cooper, extractor,
                  lock,
                  new Object(),
                  max,
                  autoCommit);
        }
        
        public void begin()
        {
            if (size() == 0)
            {
                lock();
            }
        }
        
        public void end(X txn)
        {
            save(txn, false);
            if (size() == 0)
            {
                unlock();
            }
        }
        
        public TierWriter<B, A, X> newTierCache()
        {
            return new PerQueryTierCache<T, B, A, X>(getStorage(), cooper, extractor, lock, max, isAutoCommit());
        }
    }
    
    public static class PerStrataTierWriter<T, B, A, X>
    extends AbstractTierCache<T, B, A, X>
    {
        private final ReadWriteLock readWriteLock;

        public PerStrataTierWriter(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor, int max)
        {
            this(storage, cooper, extractor, new ReentrantReadWriteLock(), new Object(), max);
        }

        private PerStrataTierWriter(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor,
                                    ReadWriteLock readWriteLock,
                                    Object mutex,
                                    int max)
        {
            this(storage, cooper, extractor,
                 readWriteLock,
                 mutex,
                 max,
                 true);
        }

        private PerStrataTierWriter(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor,
                                    ReadWriteLock readWriteLock,
                                    Object mutex,
                                    int max,
                                    boolean autoCommit)
        {
            super(storage, cooper, extractor, readWriteLock.writeLock(), mutex, max, autoCommit);
            this.readWriteLock = readWriteLock;
        }
        
        public void begin()
        {
            if (lockCount == 0)
            {
                readWriteLock.readLock().lock();
            }
        }
        
        public void end(X txn)
        {
            save(txn, false);
            if (lockCount == 0)
            {
                readWriteLock.readLock().unlock();
            }
        }
        
        public TierWriter<B, A, X> newTierCache()
        {
            return new PerStrataTierWriter<T, B, A, X>(getStorage(), cooper, extractor, readWriteLock, mutex, max, isAutoCommit());
        }
    }
       
    public interface Deletable<T>
    {
        public boolean deletable(T object);
    }
    
    public interface Addresser<A>
    {
        public <B> A getAddress(InnerTier<B, A> inner);
        
        public boolean isNull(A address);
        
        public A getNull();
    }

    public interface TierPool<B, A, X>
    {
        public LeafTier<B, A> getLeafTier(X txn, A address);
        
        public InnerTier<B, A> getInnerTier(X txn, A address);
    }
    
    private interface Unmappable
    {
        public void unmap();
    }
    
    private final static class KeyedReference<A, T>
    extends SoftReference<T> implements Unmappable
    {
        private final A key;
        
        private final Map<A, ?> map;
        
        public KeyedReference(A key, T object, Map<A, Reference<T>> map, ReferenceQueue<T> queue)
        {
            super(object, queue);
            this.key = key;
            this.map = map;
        }
        
        public void unmap()
        {
            map.remove(key);
        }
    }
    
    private final static class StorageTierPool<T, A, X, B>
    implements TierPool<B, A, X>
    {
        private final ReferenceQueue<InnerTier<B, A>> innerQueue = null;
        
        private final ReferenceQueue<LeafTier<B, A>> leafQueue = null;
        
        private final Map<A, Reference<InnerTier<B, A>>> innerTiers = null;
        
        private final Map<A, Reference<LeafTier<B, A>>> leafTiers = null;
        
        private final Storage<T, A, X> storage;
        
        private final Extractor<T, X> extractor;
        
        private final Cooper<T, B, X> cooper;
        
        public StorageTierPool(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor)
        {
            this.storage = storage;
            this.cooper = cooper;
            this.extractor = extractor;
        }
        
        private void collect()
        {
            synchronized (innerTiers)
            {
                Unmappable unmappable = null;
                while ((unmappable = (Unmappable) innerQueue.poll()) != null)
                {
                    unmappable.unmap();
                }
            }
            synchronized (leafTiers)
            {
                Unmappable unmappable = null;
                while ((unmappable = (Unmappable) leafQueue.poll()) != null)
                {
                    unmappable.unmap();
                }
            }
        }
        
        public InnerTier<B, A> getInnerTier(X txn, A address)
        {
            collect();
            
            InnerTier<B, A> inner = null;
            
            synchronized (innerTiers)
            {
                Reference<InnerTier<B, A>> reference = innerTiers.get(address);
                if (reference != null)
                {
                    inner = reference.get();
                }
                if (inner == null)
                {
                    inner = storage.getInnerStore().load(txn, address, cooper, extractor);
                    innerTiers.put(inner.getAddress(), new KeyedReference<A, InnerTier<B,A>>(address, inner, innerTiers, innerQueue));
                }
            }

            return inner;
        }
        
        public LeafTier<B, A> getLeafTier(X txn, A address)
        {
            collect();

            LeafTier<B, A> leaf = null;
            
            synchronized (leafTiers)
            {
                Reference<LeafTier<B, A>> reference = leafTiers.get(address);
                if (reference != null)
                {
                    leaf = reference.get();
                }
                if (leaf == null)
                {
                    leaf = storage.getLeafStore().load(txn, address, cooper, extractor);
                    leafTiers.put(leaf.getAddress(), new KeyedReference<A, LeafTier<B,A>>(address, leaf, leafTiers, leafQueue));
                }
            }

            return leaf;
        }
    }
    
    public static <T, A, X, B> TierPool<B, A, X> newCachingTierPool(
    Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor)
    {
        return new StorageTierPool<T, A, X, B>(storage, cooper, extractor);
    }
    
    private final static class ObjectReferenceTierPool<B, A, X>
    implements TierPool<B, A, X>
    {
        @SuppressWarnings("unchecked")
        public InnerTier<B, A> getInnerTier(X txn, A address)
        {
            return (InnerTier<B, A>) address;
        }
        
        @SuppressWarnings("unchecked")
        public LeafTier<B, A> getLeafTier(X txn, A address)
        {
            return (LeafTier<B, A>) address;
        }
    }
    
    private final static class Mutation<B, A, X>
    extends Navigator<B, A, X>
    {
        private boolean onlyChild;
        
        private boolean deleting;

        private B result;
        
        private B replacement;
        
        private LeafTier<B, A> leftLeaf;
        
        public final Comparable<B> comparable;

        public final Deletable<B> deletable;

        public final LinkedList<Level<B, A, X>> listOfLevels = new LinkedList<Level<B, A, X>>();

        public LeafOperation<B, A, X> leafOperation;

        public final B bucket;
        
        private final Structure<B, A, X> structure;

        public Mutation(Navigator<B, A, X> navigator,
                        Structure<B, A, X> structure,
                        B bucket,
                        Comparable<B> comparable,
                        Deletable<B> deletable)
        {
            super(navigator);
            this.comparable = comparable;
            this.deletable = deletable;
            this.bucket = bucket;
            this.structure = structure;
        }
        
        public TierWriter<B, A, X> getWriter()
        {
            return structure.getWriter();
        }

        public int getInnerSize()
        {
            return structure.getInnerSize();
        }
        
        public int getLeafSize()
        {
            return structure.getLeafSize();
        }
        
        public B getResult()
        {
            return result;
        }
        
        public void setResult(B result)
        {
            this.result = result;
        }
        
        public B getReplacement()
        {
            return replacement;
        }
        
        public void setReplacement(B replacement)
        {
            this.replacement = replacement;
        }
        
        public LeafTier<B, A> getLeftLeaf()
        {
            return leftLeaf;
        }
        
        public void setLeftLeaf(LeafTier<B, A> leftLeaf)
        {
            this.leftLeaf = leftLeaf;
        }
        
        public boolean isOnlyChild()
        {
            return onlyChild;
        }
        
        public void setOnlyChild(boolean onlyChild)
        {
            this.onlyChild = onlyChild;
        }
        
        public boolean isDeleting()
        {
            return deleting;
        }
        
        public void setDeleting(boolean deleting)
        {
            this.deleting = deleting;
        }
        
        public InnerTier<B, A> newInnerTier(ChildType childType)
        {
            InnerTier<B, A> inner = new InnerTier<B, A>();
            inner.setAddress(getAllocator().allocate(getTxn(), inner));
            inner.setChildType(childType);
            return inner;
        }
        
        public LeafTier<B, A> newLeafTier()
        {
            LeafTier<B, A> leaf = new LeafTier<B, A>();
            leaf.setAddress(getAllocator().allocate(getTxn(), leaf));
            return leaf;
        }
        
        public void rewind(int leaveExclusive)
        {
            Iterator<Level<B, A, X>>levels = listOfLevels.iterator();
            int size = listOfLevels.size();
            boolean unlock = true;

            for (int i = 0; i < size - leaveExclusive; i++)
            {
                Level<B, A, X> level = (Level<B, A, X>) levels.next();
                Iterator<Operation<B, A, X>> operations = level.listOfOperations.iterator();
                while (operations.hasNext())
                {
                    Operation<B, A, X> operation = (Operation<B, A, X>) operations.next();
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
            Iterator<Level<B, A, X>> levels = listOfLevels.iterator();
            while (listOfLevels.size() > 3 && levels.hasNext())
            {
                Level<B, A, X> level = levels.next();
                if (level.listOfOperations.size() != 0)
                {
                    break;
                }

                level.releaseAndClear();
                levels.remove();
            }
        }
        
        public Comparable<B> newComparable(B object)
        {
            return null;
        }
        
        public void clear()
        {
            
        }
    }


    private interface LeafOperation<B, A, X>
    {
        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf);
    }

    private interface Operation<B, A, X>
    {
        public void operate(Mutation<B, A, X> mutation);

        public boolean canCancel();
    }

    private interface RootDecision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfRoot, InnerTier<B, A> root);

        public void operation(Mutation<B, A, X> mutation, Level<B, A, X> levelOfRoot, InnerTier<B, A> root);
    }

    private interface Decision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild, InnerTier<B, A> parent);
    }
    
    public final static class SplitRoot<B, A, X>
    implements RootDecision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation,
                            Level<B, A, X> levelOfRoot,
                            InnerTier<B, A> root)
        {
            return mutation.getInnerSize() == root.size();
        }

        public void operation(Mutation<B, A, X> mutation,
                              Level<B, A, X> levelOfRoot,
                              InnerTier<B, A> root)
        {
            levelOfRoot.listOfOperations.add(new Split<B, A, X>(root));
        }

        private final static class Split<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> root;

            public Split(InnerTier<B, A> root)
            {
                this.root = root;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                InnerTier<B, A> left = mutation.newInnerTier(root.getChildType());
                InnerTier<B, A> right = mutation.newInnerTier(root.getChildType());
                
                int partition = root.size() / 2;
                int fullSize = root.size();
                for (int i = 0; i < partition; i++)
                {
                    left.add(root.remove(0));
                }
                for (int i = partition; i < fullSize; i++)
                {
                    right.add(root.remove(0));
                }
                B pivot = right.get(0).getPivot();
                right.get(0).setPivot(null);

                root.add(new Branch<B, A>(null, left.getAddress()));
                root.add(new Branch<B, A>(pivot, right.getAddress()));

                root.setChildType(ChildType.INNER);

                mutation.getWriter().dirty(mutation.getTxn(), root);
                mutation.getWriter().dirty(mutation.getTxn(), left);
                mutation.getWriter().dirty(mutation.getTxn(), right);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class SplitInner<B, A, X>
    implements Decision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild, InnerTier<B, A> parent)
        {
            Branch<B, A> branch = parent.find(mutation.comparable);
            InnerTier<B, A> child = mutation.getInnerTier(branch.getRightKey());
            levelOfChild.lockAndAdd(child);
            if (child.size() == mutation.getInnerSize())
            {
                levelOfParent.listOfOperations.add(new Split<B, A, X>(parent, child));
                return true;
            }
            return false;
        }

        public final static class Split<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> parent;

            private final InnerTier<B, A> child;

            public Split(InnerTier<B, A> parent, InnerTier<B, A> child)
            {
                this.parent = parent;
                this.child = child;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                InnerTier<B, A> right = mutation.newInnerTier(child.getChildType());

                int partition = child.size() / 2;

                while (partition < child.size())
                {
                    right.add(child.remove(partition));
                }

                B pivot = right.get(0).getPivot();
                right.get(0).setPivot(null);

                int index = parent.getIndex(child.getAddress());
                parent.add(index + 1, new Branch<B, A>(pivot, right.getAddress()));

                mutation.getWriter().dirty(mutation.getTxn(), parent);
                mutation.getWriter().dirty(mutation.getTxn(), child);
                mutation.getWriter().dirty(mutation.getTxn(), right);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class InnerNever<B, A, X>
    implements Decision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild, InnerTier<B, A> parent)
        {
            return false;
        }
    }

    private final static class LeafInsert<B, A, X>
    implements Decision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild, InnerTier<B, A> parent)
        {
            boolean split = true;
            levelOfChild.getSync = new WriteLockExtractor();
            Branch<B, A> branch = parent.find(mutation.comparable);
            LeafTier<B, A> leaf = mutation.getLeafTier(branch.getRightKey());
            levelOfChild.getSync = new WriteLockExtractor();
            levelOfChild.lockAndAdd(leaf);
            if (leaf.size() == mutation.getLeafSize())
            {
                Comparable<B> first = mutation.newComparable(leaf.get(0));
                if (first.compareTo(leaf.get(leaf.size() - 1)) == 0)
                {
                    int compare = mutation.comparable.compareTo(leaf.get(0));
                    if (compare < 0)
                    {
                        mutation.leafOperation = new SplitLinkedListLeft<B, A, X>(parent);
                    }
                    else if (compare > 0)
                    {
                        mutation.leafOperation = new SplitLinkedListRight<B, A, X>(parent);
                    }
                    else
                    {
                        mutation.leafOperation = new InsertLinkedList<B, A, X>(leaf);
                        split = false;
                    }
                }
                else
                {
                    levelOfParent.listOfOperations.add(new SplitLeaf<B, A, X>(parent));
                    mutation.leafOperation = new InsertSorted<B, A, X>(parent);
                }
            }
            else
            {
                mutation.leafOperation = new InsertSorted<B, A, X>(parent);
                split = false;
            }
            return split;
        }

        private final static class SplitLinkedListLeft<B, A, X>
        implements LeafOperation<B, A, X>
        {
            private final InnerTier<B, A> inner;

            public SplitLinkedListLeft(InnerTier<B, A> inner)
            {
                this.inner = inner;
            }

            public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
            {
                Branch<B, A> branch = inner.find(mutation.comparable);
                LeafTier<B, A> leaf = mutation.getLeafTier(branch.getRightKey());

                LeafTier<B, A> right = mutation.newLeafTier();
                while (leaf.size() != 0)
                {
                    right.add(leaf.remove(0));
                }

                leaf.link(mutation, right);

                int index = inner.getIndex(leaf.getAddress());
                if (index != 0)
                {
                    throw new IllegalStateException();
                }
                inner.add(index + 1, new Branch<B, A>(right.get(0), right.getAddress()));

                mutation.getWriter().dirty(mutation.getTxn(), inner);
                mutation.getWriter().dirty(mutation.getTxn(), leaf);
                mutation.getWriter().dirty(mutation.getTxn(), right);

                return new InsertSorted<B, A, X>(inner).operate(mutation, levelOfLeaf);
            }
        }

        private final static class SplitLinkedListRight<B, A, X>
        implements LeafOperation<B, A, X>
        {
            private final InnerTier<B, A> inner;

            public SplitLinkedListRight(InnerTier<B, A> inner)
            {
                this.inner = inner;
            }

            private boolean endOfList(Mutation<B, A, X> mutation, LeafTier<B, A> last)
            {
                return mutation.getAllocator().isNull(last.getNext()) || mutation.newComparable(last.getNext(mutation).get(0)).compareTo(last.get(0)) != 0;
            }

            public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
            {
                Branch<B, A> branch = inner.find(mutation.comparable);
                LeafTier<B, A> leaf = mutation.getLeafTier(branch.getRightKey());

                LeafTier<B, A> last = leaf;
                while (!endOfList(mutation, last))
                {
                    last = last.getNextAndLock(mutation, levelOfLeaf);
                }

                LeafTier<B, A> right = mutation.newLeafTier();
                last.link(mutation, right);

                inner.add(inner.getIndex(leaf.getAddress()) + 1, new Branch<B, A>(mutation.bucket, right.getAddress()));

                mutation.getWriter().dirty(mutation.getTxn(), inner);
                mutation.getWriter().dirty(mutation.getTxn(), leaf);
                mutation.getWriter().dirty(mutation.getTxn(), right);

                return new InsertSorted<B, A, X>(inner).operate(mutation, levelOfLeaf);
            }
        }

        private final static class SplitLeaf<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> inner;

            public SplitLeaf(InnerTier<B, A> inner)
            {
                this.inner = inner;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                Branch<B, A> branch = inner.find(mutation.comparable);
                LeafTier<B, A> leaf = mutation.getLeafTier(branch.getRightKey());

                int middle = leaf.size() >> 1;
                boolean odd = (leaf.size() & 1) == 1;
                int lesser = middle - 1;
                int greater = odd ? middle + 1 : middle;

                int partition = -1;
                Comparable<B> candidate = mutation.newComparable(leaf.get(middle));
                for (int i = 0; partition == -1 && i < middle; i++)
                {
                    if (candidate.compareTo(leaf.get(lesser)) != 0)
                    {
                        partition = lesser + 1;
                    }
                    else if (candidate.compareTo(leaf.get(greater)) != 0)
                    {
                        partition = greater;
                    }
                    lesser--;
                    greater++;
                }

                LeafTier<B, A> right = mutation.newLeafTier();

                while (partition != leaf.size())
                {
                    right.add(leaf.remove(partition));
                }

                leaf.link(mutation, right);

                int index = inner.getIndex(leaf.getAddress());
                inner.add(index + 1, new Branch<B, A>(right.get(0), right.getAddress()));

                mutation.getWriter().dirty(mutation.getTxn(), inner);
                mutation.getWriter().dirty(mutation.getTxn(), leaf);
                mutation.getWriter().dirty(mutation.getTxn(), right);
            }

            public boolean canCancel()
            {
                return true;
            }
        }

        private final static class InsertSorted<B, A, X>
        implements LeafOperation<B, A, X>
        {
            private final InnerTier<B, A> inner;

            public InsertSorted(InnerTier<B, A> inner)
            {
                this.inner = inner;
            }

            public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
            {
                Branch<B, A> branch = inner.find(mutation.comparable);
                LeafTier<B, A> leaf = mutation.getLeafTier(branch.getRightKey());

                ListIterator<B> objects = leaf.listIterator();
                while (objects.hasNext())
                {
                    B before = objects.next();
                    if (mutation.comparable.compareTo(before) <= 0)
                    {
                        objects.previous();
                        objects.add(mutation.bucket);
                        break;
                    }
                }

                if (!objects.hasNext())
                {
                    objects.add(mutation.bucket);
                }

                // FIXME Now we are writing before we are splitting. Problem.
                // Empty cache does not work!
                mutation.getWriter().dirty(mutation.getTxn(), leaf);

                return true;
            }
        }

        private final static class InsertLinkedList<B, A, X>
        implements LeafOperation<B, A, X>
        {
            private final LeafTier<B, A> leaf;

            public InsertLinkedList(LeafTier<B, A> leaf)
            {
                this.leaf = leaf;
            }

            public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
            {
                leaf.append(mutation, levelOfLeaf);
                return true;
            }
        }
    }

    /**
     * Logic for deleting the 
     */
    private final static class DeleteRoot<B, A, X>
    implements RootDecision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfRoot, InnerTier<B, A> root)
        {
            if (root.getChildType() == ChildType.INNER && root.size() == 2)
            {
                InnerTier<B, A> first = mutation.getInnerTier(root.get(0).getRightKey());
                InnerTier<B, A> second = mutation.getInnerTier(root.get(1).getRightKey());
                // FIXME These numbers are off.
                return first.size() + second.size() == mutation.getInnerSize();
            }
            return false;
        }

        public void operation(Mutation<B, A, X> mutation, Level<B, A, X> levelOfRoot, InnerTier<B, A> root)
        {
            levelOfRoot.listOfOperations.add(new Merge<B, A, X>(root));
        }

        public final static class Merge<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> root;

            public Merge(InnerTier<B, A> root)
            {
                this.root = root;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                if (root.size() != 0)
                {
                    throw new IllegalStateException();
                }

                InnerTier<B, A> child = mutation.getInnerTier(root.remove(0).getRightKey());
                while (child.size() != 0)
                {
                    root.add(child.remove(0));
                }

                root.setChildType(child.getChildType());

                mutation.getWriter().remove(child);

                mutation.getWriter().dirty(mutation.getTxn(), root);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class SwapKey<B, A, X>
    implements Decision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation,
                            Level<B, A, X> levelOfParent,
                            Level<B, A, X> levelOfChild,
                            InnerTier<B, A> parent)
        {
            Branch<B, A> branch = parent.find(mutation.comparable);
            if (branch.getPivot() != null && mutation.comparable.compareTo(branch.getPivot()) == 0)
            {
                levelOfParent.listOfOperations.add(new Swap<B, A, X>(parent));
                return true;
            }
            return false;
        }

        private final static class Swap<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> inner;

            public Swap(InnerTier<B, A> inner)
            {
                this.inner = inner;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                if (mutation.getReplacement() != null)
                {
                    Branch<B, A> branch = inner.find(mutation.comparable);
                    branch.setPivot(mutation.getReplacement());
                    mutation.getWriter().dirty(mutation.getTxn(), inner);
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
    private final static class MergeInner<B, A, X>
    implements Decision<B, A, X>
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
        private boolean lockLeft(Mutation<B, A, X> mutation, Branch<B, A> branch)
        {
            if (mutation.isOnlyChild() && branch.getPivot() != null && mutation.getLeftLeaf() == null)
            {
                return mutation.comparable.compareTo(branch.getPivot()) == 0;
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
        public boolean test(Mutation<B, A, X> mutation,
                            Level<B, A, X> levelOfParent,
                            Level<B, A, X> levelOfChild,
                            InnerTier<B, A> parent)
        {
            // Find the child tier.

            Branch<B, A> branch = parent.find(mutation.comparable);
            InnerTier<B, A> child = mutation.getInnerTier(branch.getRightKey());

            // If we are on our way down to remove the last item of a leaf
            // tier that is an only child, then we need to find the leaf to
            // the left of the only child leaf tier. This means that we need
            // to detect the branch that uses the the value of the last item in
            // the only child leaf as a pivot. When we detect it we then
            // navigate each right most branch of the tier referenced by the
            // branch before it to find the leaf to the left of the only child
            // leaf. We then make note of it so we can link it around the only
            // child that is go be removed.

            if (lockLeft(mutation, branch))
            {
                // FIXME You need to hold these exclusive locks, so add an
                // operation that is uncancelable, but does nothing.

                int index = parent.getIndex(child.getAddress()) - 1;
                InnerTier<B, A> inner = parent;
                while (inner.getChildType() == ChildType.INNER)
                {
                    inner = mutation.getInnerTier(inner.get(index).getRightKey());
                    levelOfParent.lockAndAdd(inner);
                    index = inner.size() - 1;
                }
                LeafTier<B, A> leaf = mutation.getLeafTier(inner.get(index).getRightKey());
                levelOfParent.lockAndAdd(leaf);
                mutation.setLeftLeaf(leaf);
            }


            // When we detect an inner tier with an only child, we note that
            // we have begun to descend a list of tiers with only one child.
            // Tiers with only one child are deleted rather than merged. If we
            // encounter a tier with children with siblings, we are no longer
            // deleting.

            if (child.size() == 1)
            {
                if (!mutation.isDeleting())
                {
                    mutation.setDeleting(true);
                }
                levelOfParent.listOfOperations.add(new Remove<B, A, X>(parent, child));
                return true;
            }

            // Determine if we can merge with either sibling.

            List<InnerTier<B, A>> listToMerge = new ArrayList<InnerTier<B, A>>(2);

            int index = parent.getIndex(child.getAddress());
            if (index != 0)
            {
                InnerTier<B, A> left = mutation.getInnerTier(parent.get(index - 1).getRightKey());
                levelOfChild.lockAndAdd(left);
                levelOfChild.lockAndAdd(child);
                if (left.size() + child.size() <= mutation.getInnerSize())
                {
                    listToMerge.add(left);
                    listToMerge.add(child);
                }
            }

            if (index == 0)
            {
                levelOfChild.lockAndAdd(child);
            }

            if (listToMerge.isEmpty() && index != parent.size() - 1)
            {
                InnerTier<B, A> right = mutation.getInnerTier(parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(right);
                if ((child.size() + right.size() - 1) == mutation.getInnerSize())
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

                if (mutation.isDeleting())
                {
                    mutation.rewind(2);
                    mutation.setDeleting(false);
                }

                levelOfParent.listOfOperations.add(new Merge<B, A, X>(parent, listToMerge));

                return true;
            }

            // When we encounter an inner tier without an only child, then we
            // are no longer deleting. Returning false will cause the Query to
            // rewind the exclusive locks and cancel the delete operations, so
            // the delete action is reset.

            mutation.setDeleting(false);

            return false;
        }

        public final static class Merge<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> parent;

            private final List<InnerTier<B, A>> listToMerge;

            public Merge(InnerTier<B, A> parent, List<InnerTier<B, A>> listToMerge)
            {
                this.parent = parent;
                this.listToMerge = listToMerge;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                InnerTier<B, A> left = listToMerge.get(0);
                InnerTier<B, A> right = listToMerge.get(1);

                int index = parent.getIndex(right.getAddress());
                Branch<B, A> branch = parent.remove(index);

                right.get(0).setPivot(branch.getPivot());
                while (right.size() != 0)
                {
                    left.add(right.remove(0));
                }

                mutation.getWriter().remove(right);
                mutation.getWriter().dirty(mutation.getTxn(), parent);
                mutation.getWriter().dirty(mutation.getTxn(), left);
            }

            public boolean canCancel()
            {
                return true;
            }
        }

        public final static class Remove<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> parent;

            private final InnerTier<B, A> child;

            public Remove(InnerTier<B, A> parent, InnerTier<B, A> child)
            {
                this.parent = parent;
                this.child = child;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                int index = parent.getIndex(child.getAddress());

                parent.remove(index);
                if (parent.size() != 0)
                {
                    parent.get(0).setPivot(null);
                }

                mutation.getWriter().remove(child);
                mutation.getWriter().dirty(mutation.getTxn(), parent);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    private final static class LeafRemove<B, A, X>
    implements Decision<B, A, X>
    {
        public boolean test(Mutation<B, A, X> mutation,
                            Level<B, A, X> levelOfParent,
                            Level<B, A, X> levelOfChild,
                            InnerTier<B, A> parent)
        {
            levelOfChild.getSync = new WriteLockExtractor();
            Branch<B, A> branch = parent.find(mutation.comparable);
            int index = parent.getIndex(branch.getRightKey());
            LeafTier<B, A> previous = null;
            LeafTier<B, A> leaf = null;
            List<LeafTier<B, A>> listToMerge = new ArrayList<LeafTier<B, A>>();
            if (index != 0)
            {
                previous = mutation.getLeafTier(parent.get(index - 1).getRightKey());
                levelOfChild.lockAndAdd(previous);
                leaf = mutation.getLeafTier(branch.getRightKey());
                levelOfChild.lockAndAdd(leaf);
                int capacity = previous.size() + leaf.size();
                if (capacity <= mutation.getLeafSize() + 1)
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
                leaf = mutation.getLeafTier(branch.getRightKey());
                levelOfChild.lockAndAdd(leaf);
            }

            // TODO Do not need the parent size test, just need deleting.
            if (leaf.size() == 1 && parent.size() == 1 && mutation.isDeleting())
            {
                LeafTier<B, A> left = mutation.getLeftLeaf();
                if (left == null)
                {
                    mutation.setOnlyChild(true);
                    mutation.leafOperation = new Fail<B, A, X>();
                    return false;
                }

                levelOfParent.listOfOperations.add(new RemoveLeaf<B, A, X>(parent, leaf, left));
                mutation.leafOperation = new Remove<B, A, X>(leaf);
                return true;
            }
            else if (listToMerge.isEmpty() && index != parent.size() - 1)
            {
                LeafTier<B, A> next = mutation.getLeafTier(parent.get(index + 1).getRightKey());
                levelOfChild.lockAndAdd(next);
                int capacity = next.size() + leaf.size();
                if (capacity <= mutation.getLeafSize() + 1)
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
                    leaf = mutation.getLeafTier(branch.getRightKey());
                    levelOfChild.lockAndAdd(leaf);
                }
                mutation.leafOperation = new Remove<B, A, X>(leaf);
            }
            else
            {
                // TODO Test that this activates.
                if (mutation.isDeleting())
                {
                    mutation.rewind(2);
                    mutation.setDeleting(false);
                }
                LeafTier<B, A> left = listToMerge.get(0);
                LeafTier<B, A> right = listToMerge.get(1);
                levelOfParent.listOfOperations.add(new Merge<B, A, X>(parent, left, right));
                mutation.leafOperation = new Remove<B, A, X>(leaf);
            }
            return !listToMerge.isEmpty();
        }

        public final static class Remove<B, A, X>
        implements LeafOperation<B, A, X>
        {
            private final LeafTier<B, A> leaf;

            public Remove(LeafTier<B, A> leaf)
            {
                this.leaf = leaf;
            }

            public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
            {
                // TODO Remove single anywhere but far left.
                // TODO Remove single very left most.
                // TODO Remove single very right most.
                int count = 0;
                int found = 0;
                LeafTier<B, A> current = leaf;
                SEARCH: do
                {
                    Iterator<B> objects = leaf.iterator();
                    while (objects.hasNext())
                    {
                        count++;
                        B candidate = objects.next();
                        int compare = mutation.comparable.compareTo(candidate);
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
                                        LeafTier<B, A> following = current.getNextAndLock(mutation, levelOfLeaf);
                                        if (following != null)
                                        {
                                            mutation.setReplacement(following.get(0));
                                        }
                                    }
                                }
                            }
                            mutation.getWriter().dirty(mutation.getTxn(), current);
                            mutation.setResult(candidate);
                            break SEARCH;
                        }
                    }
                    current = current.getNextAndLock(mutation, levelOfLeaf);
                }
                while (current != null && mutation.comparable.compareTo(current.get(0)) == 0);

                if (mutation.getResult() != null
                    && count == found
                    && current.size() == mutation.getLeafSize() - 1
                    && mutation.comparable.compareTo(current.get(current.size() - 1)) == 0)
                {
                    for (;;)
                    {
                        LeafTier<B, A> subsequent = current.getNextAndLock(mutation, levelOfLeaf);
                        if (subsequent == null || mutation.comparable.compareTo(subsequent.get(0)) != 0)
                        {
                            break;
                        }
                        current.add(subsequent.remove(0));
                        if (subsequent.size() == 0)
                        {
                            current.setNext(subsequent.getNext());
                            mutation.getWriter().remove(subsequent);
                        }
                        else
                        {
                            mutation.getWriter().dirty(mutation.getTxn(), subsequent);
                        }
                        current = subsequent;
                    }
                }

                return mutation.getResult() != null;
            }
        }

        public final static class Fail<B, A, X>
        implements LeafOperation<B, A, X>
        {
            public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
            {
                return false;
            }
        }

        public final static class Merge<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> parent;

            private final LeafTier<B, A> left;

            private final LeafTier<B, A> right;

            public Merge(InnerTier<B, A> parent, LeafTier<B, A> left, LeafTier<B, A> right)
            {
                this.parent = parent;
                this.left = left;
                this.right = right;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                parent.remove(parent.getIndex(right.getAddress()));

                while (right.size() != 0)
                {
                    left.add(right.remove(0));
                }
                // FIXME Get last leaf. 
                left.setNext(right.getNext());

                mutation.getWriter().remove(right);
                mutation.getWriter().dirty(mutation.getTxn(), parent);
                mutation.getWriter().dirty(mutation.getTxn(), left);
            }

            public boolean canCancel()
            {
                return true;
            }
        }

        public final static class RemoveLeaf<B, A, X>
        implements Operation<B, A, X>
        {
            private final InnerTier<B, A> parent;

            private final LeafTier<B, A> leaf;

            private final LeafTier<B, A> left;

            public RemoveLeaf(InnerTier<B, A> parent, LeafTier<B, A> leaf, LeafTier<B, A> left)
            {
                this.parent = parent;
                this.leaf = leaf;
                this.left = left;
            }

            public void operate(Mutation<B, A, X> mutation)
            {
                parent.remove(parent.getIndex(leaf.getAddress()));

                left.setNext(leaf.getNext());

                mutation.getWriter().remove(leaf);
                mutation.getWriter().dirty(mutation.getTxn(), parent);
                mutation.getWriter().dirty(mutation.getTxn(), left);

                mutation.setOnlyChild(false);
            }

            public boolean canCancel()
            {
                return true;
            }
        }
    }

    public final static class CoreCursor<B, A, X>
    implements Cursor<B>
    {
        private final Navigator<B, A, X> navigator;
        
        private int index;

        private LeafTier<B, A> leaf;

        private boolean released;

        public CoreCursor(Navigator<B, A, X> navigator, LeafTier<B, A> leaf, int index)
        {
            this.navigator = navigator;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isForward()
        {
            return true;
        }

        public Cursor<B> newCursor()
        {
            return new CoreCursor<B, A, X>(navigator, leaf, index);
        }

        public boolean hasNext()
        {
            return index < leaf.size() || !navigator.getAllocator().isNull(leaf.getNext());
        }

        public B next()
        {
            if (released)
            {
                throw new IllegalStateException();
            }
            if (index == leaf.size())
            {
                if (navigator.getAllocator().isNull(leaf.getNext()))
                {
                    throw new IllegalStateException();
                }
                LeafTier<B, A> next = navigator.getLeafTier(leaf.getNext());
                next.getReadWriteLock().readLock().lock();
                leaf.getReadWriteLock().readLock().unlock();
                leaf = next;
                index = 0;
            }
            B object = leaf.get(index++);
            if (!hasNext())
            {
                release();
            }
            return object;
        }
        
        public void remove()
        {
            throw new UnsupportedOperationException();
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

    public final static class BucketCursor<T, B, X>
    implements Cursor<T>
    {
        public Cooper<T, B, X> cooper;

        private Cursor<B> cursor;

        public BucketCursor(Cursor<B> cursor)
        {
        }

        public T next()
        {
            return cooper.getObject(cursor.next());
        }

        public boolean hasNext()
        {
            return cursor.hasNext();
        }

        public void remove()
        {
        }
    }

    public interface CursorWrapper<T, B>
    {
        public Cursor<T> wrap(Cursor<B> cursor);
    }

    public interface Query<T>
    {
        public void add(T object);
        
        public Cursor<T> find(Comparable<?>... fields);
    }
    
    public interface Transaction<T, X>
    extends Query<T>
    {
        public Tree<T, X> getTree();
    }

    public final static class CoreQuery<T, A, X, B>
    implements Transaction<T, X>
    {
        private final CoreTree<T, A, X, B> tree;
        
        private final A root;
        
        private final Navigator<B, A, X> navigator;
        
        public CoreQuery(CoreTree<T, A, X, B> tree,
                         A root,
                         Navigator<B, A, X> navigator)
        {
            this.tree = tree;
            this.root = root;
            this.navigator = navigator;
        }
        
        public Tree<T, X> getTree()
        {
            return tree;
        }
                         
        private InnerTier<B, A> getRoot()
        {
            return navigator.getInnerTier(root);
        }

        private void testInnerTier(Mutation<B, A, X> mutation,
                                   Decision<B, A, X> subsequent,
                                   Decision<B, A, X> swap,
                                   Level<B, A, X> levelOfParent,
                                   Level<B, A, X> levelOfChild,
                                   InnerTier<B, A> parent, int rewind)
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
        private B generalized(Mutation<B, A, X> mutation,
                              RootDecision<B, A, X> initial,
                              Decision<B, A, X> subsequent,
                              Decision<B, A, X> swap,
                              Decision<B, A, X> penultimate)
        {
            // TODO Replace this with our caching pattern.

            // Inform the tier cache that we are about to perform a mutation
            // of the tree.
            mutation.getWriter().begin();

            mutation.listOfLevels.add(new Level<B, A, X>(false));

            InnerTier<B, A> parent = getRoot();
            Level<B, A, X> levelOfParent = new Level<B, A, X>(false);
            levelOfParent.lockAndAdd(parent);
            mutation.listOfLevels.add(levelOfParent);

            Level<B, A, X> levelOfChild = new Level<B, A, X>(false);
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
                if (parent.getChildType() == ChildType.INNER)
                {
                    testInnerTier(mutation, subsequent, swap, levelOfParent, levelOfChild, parent, 0);
                    Branch<B, A> branch = parent.find(mutation.comparable);
                    InnerTier<B, A> child = navigator.getInnerTier(branch.getRightKey());
                    parent = child;
                }
                else
                {
                    testInnerTier(mutation, penultimate, swap, levelOfParent, levelOfChild, parent, 1);
                    break;
                }
                levelOfParent = levelOfChild;
                levelOfChild = new Level<B, A, X>(levelOfChild.getSync.isExeclusive());
                mutation.listOfLevels.add(levelOfChild);
                mutation.shift();
            }

            if (mutation.leafOperation.operate(mutation, levelOfChild))
            {
                ListIterator<Level<B, A, X>> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
                while (levels.hasPrevious())
                {
                    Level<B, A, X> level = (Level<B, A, X>) levels.previous();
                    ListIterator<Operation<B, A, X>> operations = level.listOfOperations.listIterator(level.listOfOperations.size());
                    while (operations.hasPrevious())
                    {
                        Operation<B, A, X> operation = operations.previous();
                        operation.operate(mutation);
                    }
                }

                mutation.getWriter().end(navigator.getTxn());
            }

            ListIterator<Level<B, A, X>> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
            while (levels.hasPrevious())
            {
                Level<B, A, X> level = levels.previous();
                level.releaseAndClear();
            }

            return mutation.getResult();
        }
        
        public void add(T object)
        {
            CoreRecord record = new CoreRecord();
            tree.getExtractor().extract(navigator.getTxn(), object, record);
            B bucket = tree.getCooper().newBucket(record.getFields(), object);
            BucketComparable<T, B, X> comparable  = new BucketComparable<T, B, X>();
            Mutation<B, A, X> mutation = new Mutation<B, A, X>(navigator, tree, bucket, comparable, null);
            generalized(mutation, new SplitRoot<B, A, X>(), new SplitInner<B, A, X>(), new InnerNever<B, A, X>(), new LeafInsert<B, A, X>());
        }

        // TODO Where do I actually use deletable? Makes sense, though. A
        // condition to choose which to delete.
        public Object remove(Deletable<B> deletable, Comparable<?>... fields)
        {
            BucketComparable<T, B, X> comparable  = new BucketComparable<T, B, X>();
            Mutation<B, A, X> mutation = new Mutation<B, A, X>(navigator, tree, null, comparable, deletable);
            do
            {
                mutation.listOfLevels.clear();

                mutation.clear();

                generalized(mutation,
                            new DeleteRoot<B, A, X>(),
                            new MergeInner<B, A, X>(),
                            new SwapKey<B, A, X>(),
                            new LeafRemove<B, A, X>());
            }
            while (mutation.isOnlyChild());

            B removed = mutation.getResult();

            return removed;
        }

        // Here is where I get the power of not using comparator.
        public Cursor<T> find(Comparable<?>... fields)
        {
            Lock previous = new ReentrantLock();
            previous.lock();
            InnerTier<B, A> inner = getRoot();
            Comparable<B> comparator = new BucketComparable<T, B, X>();
            for (;;)
            {
                inner.getReadWriteLock().readLock().lock();
                previous.unlock();
                previous = inner.getReadWriteLock().readLock();
                Branch<B, A> branch = inner.find(comparator);
                if (inner.getChildType() == ChildType.LEAF)
                {
                    LeafTier<B, A> leaf = navigator.getLeafTier(branch.getRightKey());
                    leaf.getReadWriteLock().readLock().lock();
                    previous.unlock();
                    return tree.getCooper().wrap(leaf.find(comparator));
                }
                inner = navigator.getInnerTier(branch.getRightKey());
            }
        }
    }

    public interface Tree<T, X>
    {
        public Query<T> query(X txn);
    }
    
    interface Structure<B, A, X>
    {
        public int getInnerSize();
        
        public int getLeafSize();
        
        public TierWriter<B, A, X> getWriter();
    }
    
    public final static class CoreTree<T, A, X, B>
    implements Tree<T, X>, Structure<B, A, X>
    {
        private int innerSize;
        
        private int leafSize;

        private TierPool<B, A, X> pool;
        
        private TierWriter<B, A, X> writer;
        
        private Allocator<B, A, X> allocator;
        
        private final Cooper<T, B, X> cooper;
        
        private final Extractor<T, X> extractor;
        
        public CoreTree(int innerSize,
                        int leafSize,
                        StorageBuilder newStorage,
                        Cooper<T, B, X> cooper,
                        Extractor<T, X> extractor)
        {
            BAX<B, A, X> bax = new BAX<B, A, X>();
            this.innerSize = innerSize;
            this.leafSize = leafSize;
            this.pool = newStorage.newTierPool(bax);
            this.writer = newStorage.newTierWriter(bax);
            this.allocator = newStorage.newAllocator(bax);
            this.cooper = cooper;
            this.extractor = extractor;
        }

        public Transaction<T, X> query(X txn)
        {
            Navigator<B, A, X> navigator = new Navigator<B, A, X>(null);
            return new CoreQuery<T, A, X, B>(this, null, navigator);
        }
        
        public int getInnerSize()
        {
            return innerSize;
        }
        
        public int getLeafSize()
        {
            return leafSize;
        }
        
        public Allocator<B, A, X> getAllocator()
        {
            return allocator;
        }
        
        public TierPool<B, A, X> getPool()
        {
            return pool;
        }
        
        public TierWriter<B, A, X> getWriter()
        {
            return writer;
        }
        
        public Cooper<T, B, X> getCooper()
        {
            return cooper;
        }
        
        public Extractor<T, X> getExtractor()
        {
            return extractor;
        }
    }
    
    public interface TreeBuilder
    {
        public <T, A, X> Tree<T, X> newTree(StorageBuilder newStorage, Extractor<T, X> extractor, Class<A> addressClass);
    }
    
    private static class LookupTreeBuilder
    implements TreeBuilder
    {
        public <T, A, X> Tree<T, X> newTree(StorageBuilder newStorage, Extractor<T, X> extractor, Class<A> addressClass)
        {
            return null;
        }
    }
    
    public static final class BAX<B, A, X>
    {
    }
    
    private static class BucketTreeBuilder
    implements TreeBuilder
    {
        public <T, A, X> Tree<T, X> newTree(
            StorageBuilder newStorage, Extractor<T, X> extractor,
            Class<A> addressClass)
        {
            Cooper<T, Bucket<T> , X> cooper = new BucketCooper<T, X>();
            CoreTree<T, A, X, Bucket<T>> tree = new CoreTree<T, A, X, Bucket<T>>(0, 0, newStorage, cooper, extractor); 
            return tree;
        }
    }
    
    public interface StorageBuilder
    {
        public <T, X> Tree<T, X> newTree(TreeBuilder builder, Extractor<T, X> extractor, int innerSize, int leafSize);
        
        public <B, A, X> Allocator<B, A, X> newAllocator(BAX<B, A, X> bax);
        
        public <B, A, X> TierPool<B, A, X> newTierPool(BAX<B, A, X> bax);
        
        public <B, A, X> TierWriter<B, A, X> newTierWriter(BAX<B, A, X> bax);
    }

    public final static class CoreSchema2<T, A, X>
    implements Schema<T, X> 
    {
        private int innerSize;
        
        private int leafSize;
        
        private TreeBuilder newTree = new LookupTreeBuilder();
        
        private Extractor<T, X> extractor = null;
        
        private StorageBuilder newStorage;
        
        public CoreSchema2(StorageBuilder newStorage)
        {
            this.newStorage = newStorage;
        }
        
        public void setInnerSize(int innerSize)
        {
            this.innerSize = innerSize;
        }
        
        public void setLeafSize(int leafSize)
        {
            this.leafSize = leafSize;
        }
        
        public void setCacheFields(boolean cacheFields)
        {
            this.newTree = new BucketTreeBuilder();
        }
        
        public void setExtractor(Extractor<T, X> extractor)
        {
            this.extractor = extractor;
        }
        
        public Tree<T, X> newTree()
        {
            return newStorage.newTree(newTree, extractor, innerSize, leafSize);
        }
    }
    
    public final static class CoreSchema<T, A, X, B>
    implements Serializable
    {
        private static final long serialVersionUID = 1L;

        private int innerSize;
        
        private int leafSize;
        
        private TierWriter<B, A, X> writer;
        
        private Allocator<B, A, X> allocator;
        
        public CoreSchema()
        {
            this.leafSize = 5;
            this.innerSize = 5;
        }
        
        public int getInnerSize()
        {
            return innerSize;
        }
        
        public void setInnerSize(int innerSize)
        {
            this.innerSize = innerSize;
        }
        
        public int getLeafSize()
        {
            return leafSize;
        }
        
        public void setLeafSize(int leafSize)
        {
            this.leafSize = leafSize;
        }
        
        public Transaction<T, X> newTransaction(X txn)
        {
            writer.begin();
            
            InnerTier<B, A> root = new InnerTier<B, A>();
            root.setChildType(ChildType.LEAF);
            root.setAddress(allocator.allocate(txn, root));
            writer.dirty(txn, root);
            
            LeafTier<B, A> leaf = new LeafTier<B, A>();
            leaf.setAddress(allocator.allocate(txn, leaf));
            writer.dirty(txn, leaf);

            writer.end(txn);
            
            root.add(new Branch<B, A>(null, leaf.getAddress()));

//            Navigator<B, A, X> navigator = new Navigator<B, A, X>(txn, allocator,pool);
//            CoreTree<T, A, X, B> tree = new CoreTree<T, A, X, B>(innerSize, leafSize, 
//            CoreQuery<T, A, X, B> query = new CoreQuery<T, A, X, B>(this, tree, root.getAddress(), navigator, cooper, extractor, writer);
            
            return null;
        }
    }
    
    public interface Schema<T, X>
    {
        public void setInnerSize(int size);
        
        public void setLeafSize(int size);
        
        public void setCacheFields(boolean cacheFields);
        
        public void setExtractor(Extractor<T, X> extractor);
        
        public Tree<T, X> newTree();
    }
    
    public static <T, A, X> Schema<T, X> newSchema(Storage<T, A, X> storage)
    {
        return null;
    }
    
    public static <T> Schema<T, Object> newInMemoryTree(Class<T> klass)
    {
        return new CoreSchema2<T, Object, Object>(new InMemoryStoreBuilder());
    }
    
    private final static class InMemoryStoreBuilder
    implements StorageBuilder
    {
        public <T, X> Tree<T, X> newTree(TreeBuilder newTree, Extractor<T, X> extractor, int innerSize, int leafSize)
        {
            return newTree.newTree(this, extractor, Object.class);
        }
        
        @SuppressWarnings("unchecked")
        public <B, A, X> Allocator<B, A, X> newAllocator(BAX<B, A, X> bax)
        {
            return (Strata.Allocator<B, A, X>) new NullAllocator<B>();
        }
        
        public <B, A, X> TierPool<B, A, X> newTierPool(BAX<B, A, X> bax)
        {
            return new ObjectReferenceTierPool<B, A, X>();
        }
        
        public <B, A, X> TierWriter<B, A, X> newTierWriter(BAX<B, A, X> bax)
        {
            return new EmptyTierCache<B, A, X>();
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */