/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.zstd;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ZstdCodec
        implements Configurable, CompressionCodec
{
    private Configuration conf;

    @Override
    public Configuration getConf()
    {
        return conf;
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException
    {
        return new HadoopZstdOutputStream(out);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        if (!(compressor instanceof HadoopZstdCompressor)) {
            throw new IllegalArgumentException("Compressor is not the ZSTD compressor");
        }
        return new HadoopZstdOutputStream(out);
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        return HadoopZstdCompressor.class;
    }

    @Override
    public Compressor createCompressor()
    {
        return new HadoopZstdCompressor();
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return new HadoopZstdInputStream(in);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        if (!(decompressor instanceof HadoopZstdDecompressor)) {
            throw new IllegalArgumentException("Decompressor is not the ZSTD decompressor");
        }
        return new HadoopZstdInputStream(in);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return HadoopZstdDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return new HadoopZstdDecompressor();
    }

    @Override
    public String getDefaultExtension()
    {
        return ".zst";
    }

    /**
     * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
     * with a compressor can function.  This interface can be implemented if needed.
     */
    @DoNotPool
    private static class HadoopZstdCompressor
            implements Compressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public long getBytesWritten()
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public void finish()
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public int compress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("ZSTD block compressor is not supported");
        }

        @Override
        public void reset() {}

        @Override
        public void end() {}

        @Override
        public void reinit(Configuration conf) {}
    }

    /**
     * No Hadoop code seems to actually use the decompressor, so just return a dummy one so the createInputStream method
     * with a decompressor can function.  This interface can be implemented if needed, but would require modifying the
     * ZSTD decompress method to resize the output buffer, since the Hadoop block decompressor does not get the uncompressed
     * size.
     */
    @DoNotPool
    private static class HadoopZstdDecompressor
            implements Decompressor
    {
        @Override
        public void setInput(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public boolean needsInput()
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public void setDictionary(byte[] b, int off, int len)
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public boolean needsDictionary()
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public boolean finished()
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public int decompress(byte[] b, int off, int len)
                throws IOException
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public int getRemaining()
        {
            throw new UnsupportedOperationException("ZSTD block decompressor is not supported");
        }

        @Override
        public void reset() {}

        @Override
        public void end() {}
    }
}
