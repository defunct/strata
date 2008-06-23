/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
implements Comparable<Pair<A, B>>
{
    private final A first;
    
    private final B second;
    
    public Pair(A first, B second)
    {
        this.first = first;
        this.second = second;
    }

    public int compareTo(Pair<A, B> pair)
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

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */