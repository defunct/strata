/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata.hash;

import java.net.URI;

import com.agtrz.sheaf.SheafLock;
import com.agtrz.sheaf.SheafLocker;
import com.agtrz.strata.Index;
import com.agtrz.strata.IndexOpener;
import com.agtrz.strata.StrataIOException;
import com.agtrz.swag.io.ObjectReadBuffer;

/**
 * @author Alan Gutierez
 */
final class HashIndexOpener
implements IndexOpener
{
    public Index open(URI uriOfIndex, SheafLocker locker)
    {
        SheafLock shared = locker.newSharedLock();
        try
        {
            int pageOfHeader = locker.getSheaf()
                                     .getSchema()
                                     .getStaticPageNumber(uriOfIndex);
            ObjectReadBuffer input = shared.getRead().read(pageOfHeader);
            /* int pageOfHashes = */ input.readInteger();
            String className = input.readString();
            try
            {
                return new HashIndex(getClass().getClassLoader()
                                               .loadClass(className));
            }
            catch (ClassNotFoundException e)
            {
                throw new StrataIOException(e).raise(HashIndexFactory.class)
                                              .newDanger("class.not.found");
            }
        }
        finally
        {
            shared.release();
        }
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
