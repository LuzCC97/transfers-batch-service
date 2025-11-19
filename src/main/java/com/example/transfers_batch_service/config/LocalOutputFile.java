package com.example.transfers_batch_service.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;

public class LocalOutputFile implements OutputFile {
    private final FileSystem fs;
    private final Path path;

    public static LocalOutputFile fromPath(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        return new LocalOutputFile(fs, path);
    }

    private LocalOutputFile(FileSystem fs, Path path) {
        this.fs = fs;
        this.path = path;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new HadoopPositionOutputStream(
                fs.create(path, true, 4096, fs.getDefaultReplication(path), blockSizeHint, null)
        );
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new HadoopPositionOutputStream(
                fs.create(path, false, 4096, fs.getDefaultReplication(path), blockSizeHint, null)
        );
    }

    @Override
    public boolean supportsBlockSize() {
        return true;
    }

    @Override
    public long defaultBlockSize() {
        return 32 * 1024 * 1024; // 32MB default block size
    }

    @Override
    public String getPath() {
        return path.toString();
    }

    // Helper class to wrap Hadoop's OutputStream to PositionOutputStream
    private static class HadoopPositionOutputStream extends PositionOutputStream {
        private final org.apache.hadoop.fs.FSDataOutputStream out;

        public HadoopPositionOutputStream(org.apache.hadoop.fs.FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}