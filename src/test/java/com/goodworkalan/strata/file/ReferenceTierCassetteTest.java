package com.goodworkalan.strata.file;

import java.nio.ByteBuffer;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 * Unit tests for the {@link ReferenceTierCassette} class.
 *
 * @author Alan Gutierrez
 */
public class ReferenceTierCassetteTest {
    /** Test read and write. */
    @Test
    public void io() {
        ByteBuffer bytes = ByteBuffer.allocate(1024);
        RecordIO recordIO = new RecordIO();
        assertEquals(recordIO.getSize(), 16);
        bytes.put(4, (byte) 0);
        ReferenceTierCassette<Record> cassette = new ReferenceTierCassette<Record>(recordIO, bytes, 4);
        assertFalse(cassette.isLeaf());
        cassette.setLeaf(true);
        assertEquals(bytes.get(4), 1);
    }
}
