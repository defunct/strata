package com.agtrz.strata;

import com.agtrz.swag.io.ObjectReadBuffer;
import com.agtrz.swag.io.ObjectWriteBuffer;

public interface Stratifier
{
    public Object getReference(Object object);

    public Object deserialize(ObjectReadBuffer input);

    public void serialize(ObjectWriteBuffer output, Object object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */