/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class Pod
{
    public static <A extends Comparable<? super A>, B extends Comparable<? super B>>
    Two<A, B> box(A one, B two)
    {
        return new Two<A, B>(one, two);
    }

    public static <A extends Comparable<? super A>, B extends Comparable<? super B>, C extends Comparable<? super C>>
    Three<A, B, C> box(A one, B two, C three)
    {
        return new Three<A, B, C>(one, two, three);
    }

    public static class Two<A extends Comparable<? super A>, B extends Comparable<? super B>>
    implements Comparable<Two<A, B>>
    {
        private final A one;
    
        private final B two;
    
        public Two(A first, B second)
        {
            this.one = first;
            this.two = second;
        }

        public int compareTo(Two<A, B> pair)
        {
            int compareA = one.compareTo(pair.one);
            if (compareA == 0)            
            {
                return two.compareTo(pair.two);
            }
            return compareA;
        }
        
        public A getOne()
        {
            return one;
        }
        
        public B getTwo()
        {
            return two;
        }
    }

    public static class Three<
        A extends Comparable<? super A>,
        B extends Comparable<? super B>,
        C extends Comparable<? super C>>
    implements Comparable<Three<A, B, C>>
    {
        private final A one;
    
        private final B two;
        
        private final C three;
    
        public Three(A one, B two, C three)
        {
            this.one = one;
            this.two = two;
            this.three = three;
        }

        public int compareTo(Three<A, B, C> pod)
        {
            int compareA = one.compareTo(pod.one);
            if (compareA == 0)            
            {
                int compareB = two.compareTo(pod.two);
                if (compareB == 0)
                {
                    return three.compareTo(pod.three);
                }
            }
            return compareA;
        }
        
        public A getOne()
        {
            return one;
        }
        
        public B getTwo()
        {
            return two;
        }
        
        public C getThree()
        {
            return three;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */