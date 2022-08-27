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

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

class HadoopZstdInputStream
        extends CompressionInputStream
{
    private ZstdInputStream zstdInputStream;

    public HadoopZstdInputStream(InputStream inputStream)
            throws IOException
    {
        super(inputStream);
        zstdInputStream = new ZstdInputStream(inputStream);
    }

    @Override
    public int read()
            throws IOException
    {
        return zstdInputStream.read();
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        return zstdInputStream.read(b);
    }

    @Override
    public int read(byte[] outputBuffer, int outputOffset, int outputLength)
            throws IOException
    {
        return zstdInputStream.read(outputBuffer, outputOffset, outputLength);
    }

    @Override
    public int available()
            throws IOException
    {
        return zstdInputStream.available();
    }

    @Override
    public void close()
            throws IOException
    {
        zstdInputStream.close();
    }

    @Override
    public void resetState()
            throws IOException
    {
        zstdInputStream = new ZstdInputStream(in);
    }
}
