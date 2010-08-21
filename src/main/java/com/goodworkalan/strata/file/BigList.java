package com.goodworkalan.strata.file;

import java.nio.MappedByteBuffer;
import java.util.AbstractList;

public class BigList<T> extends AbstractList<T> {
    private final ElementIO<T> io;
    
    private final MappedByteBuffer bytes;
    
    private int size;
    
    public BigList(ElementIO<T> io, MappedByteBuffer bytes) {
        this.io = io;
        this.bytes = bytes;
    }

    @Override
    public T get(int index) {
        return io.read(bytes, index * io.getRecordLength());
    }
    
    @Override
    public T set(int index, T item) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        T result = get(index);
        io.write(bytes, index * io.getRecordLength(), item);
        return result;
    }
    
    @Override
    public void add(int index, T element) {
        size++;
        for (int i = size - 2; i >= index; i--) {
            set(index + 1, get(index));
        }
        set(index, element);
    }

    @Override
    public int size() {
        return size;
    }
}
