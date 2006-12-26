/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.Comparator;

public class Equals
implements BinaryOperator
{
    private final Comparator comparator;

    public Equals(Comparator comparator)
    {
        this.comparator = comparator;
    }

    public boolean operate(Object leftOperand, Object rightOperatnd)
    {
        return comparator.compare(leftOperand, rightOperatnd) == 0;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */