package org.example.streamLoad;

import java.io.IOException;
import java.io.InputStream;

public class RecordStream extends InputStream {
    private final RecordBuffer recordBuffer;

    @Override
    public int read() throws IOException {
        return 0;
    }

    public RecordStream(int bufferSize, int bufferCount) {
        this.recordBuffer = new RecordBuffer(bufferSize, bufferCount);
    }

    public void startInput() {
        recordBuffer.startBufferData();
    }

    public void endInput() throws IOException{
        recordBuffer.stopBufferData();
    }

    @Override
    public int read(byte[] buff) throws IOException {
        try {
            return recordBuffer.read(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(byte[] buff) throws IOException {
        try {
            recordBuffer.write(buff);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
