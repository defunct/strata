/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata.hash;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Comparator;

import com.agtrz.sheaf.Address;
import com.agtrz.sheaf.NIO;
import com.agtrz.sheaf.PageRecorder;
import com.agtrz.sheaf.SheafBuilder;
import com.agtrz.sheaf.Write;
import com.agtrz.strata.IndexCreator;
import com.agtrz.strata.StrataCreateException;
import com.agtrz.swag.SizeOf;
import com.agtrz.swag.danger.Danger;

/**
 * @author Alan Gutierez
 */
public class HashIndexCreator
implements IndexCreator
{
    private final static int HEADER_SIZE = Address.SIZE;
    
    private final int sizeOfClassName(Class klass)
    {
        return SizeOf.INTEGER + (klass.getName().length() * SizeOf.CHAR);
    }

    public void create(URI uriOfIndex,
                       SheafBuilder newSheaf,
                       Class comparatorClass)
    {
        if (Comparator.class.isAssignableFrom(comparatorClass))
        {
            Danger danger = Danger.raise(HashIndexFactory.class)
                                  .newDanger("not.a.comparator");
            throw new StrataCreateException(danger);
        }
        
        int size = HEADER_SIZE + sizeOfClassName(comparatorClass);
        PageRecorder recorder = new IndexPageRecorder(comparatorClass);
        
        newSheaf.addStaticPage(uriOfIndex, size, recorder);
    }
    
    private final static class IndexPageRecorder
    implements PageRecorder
    {
        private final Class comparatorClass;
        
        public IndexPageRecorder(Class comparatorClass)
        {
            this.comparatorClass = comparatorClass;
        }

        public void record(Write write, int page)
        {
            ByteBuffer bytes = write.write(page);
            bytes.putLong(0L);
            NIO.putCharSequence(bytes, comparatorClass.getName());
        }
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
