package com.goodworkalan.strata.file;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;

/**
 * Testing memory mapping of files.
 *
 * @author Alan Gutierrez
 */
public class MapFileTest {
    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("hello.raf", "rw");
        int size = 1024 * 1024 * 1024; // 1 GB
        raf.setLength(size);
        FileChannel channel = raf.getChannel();
        MappedByteBuffer map = channel.map(MapMode.READ_WRITE, 0, size);
        raf.close();
        map.put(size - 1, (byte) 7);
        map.force();
        channel.close();
        raf = new RandomAccessFile("target/raf.data", "rw");
        channel = raf.getChannel();
        map = channel.map(MapMode.READ_WRITE, 0, size);
        raf.close();
        assertEquals(map.get(size -1), 7);
        channel.close();
    }
}
