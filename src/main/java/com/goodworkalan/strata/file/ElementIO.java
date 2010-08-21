package com.goodworkalan.strata.file;

import java.nio.ByteBuffer;

public interface ElementIO<T> {
    public void write(ByteBuffer bytes, int index, T item);
    public T read(ByteBuffer bytes, int index);
    public int getRecordLength();
}
