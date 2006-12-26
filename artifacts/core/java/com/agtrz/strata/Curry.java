/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class Curry
implements UrnaryOperator
{
    private final Object leftOperand;
    
    private final BinaryOperator operator;
    
    public Curry(BinaryOperator operator, Object leftOperand)
    {
        this.operator = operator;
        this.leftOperand = leftOperand;
    }

    public boolean operate(Object rightOperand)
    {
        return operator.operate(leftOperand, rightOperand);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */