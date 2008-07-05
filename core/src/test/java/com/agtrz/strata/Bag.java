/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public abstract class Bag<O, E extends Comparable<? super E>>
{
    public interface Extractor<O, E extends Comparable<? super E>>
    {
        E extract(O o);
    }
    
    public static <A extends Comparable<? super A>, B extends Comparable<? super B>>
    Two<A, B> box(A first, B second)
    {
        return new Two<A, B>(first, second);
    }

    public static class Two<A extends Comparable<? super A>, B extends Comparable<? super B>>
    implements Comparable<Two<A, B>>
    {
        private final A first;
    
        private final B second;
    
        public Two(A first, B second)
        {
            this.first = first;
            this.second = second;
        }

        public int compareTo(Two<A, B> pair)
        {
            int compareA = first.compareTo(pair.first);
            if (compareA == 0)            
            {
                return second.compareTo(pair.second);
            }
            return compareA;
        }
        
        public A getFirst()
        {
            return first;
        }
        
        public B getSecond()
        {
            return second;
        }
    }
   
    public abstract void add(O o);
    
    public abstract O get(O o);
    
    public abstract boolean contains(O o);
    
    public abstract Cursor<O> find(E e);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */