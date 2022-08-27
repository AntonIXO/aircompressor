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

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

class HadoopZstdOutputStream
        extends CompressionOutputStream
{
    private ZstdOutputStream zstdOutputStream;

    public HadoopZstdOutputStream(OutputStream outputStream)
            throws IOException
    {
        super(outputStream);
        zstdOutputStream = new ZstdOutputStream(outputStream);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        zstdOutputStream.write(b);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        zstdOutputStream.write(buffer, offset, length);
    }

    @Override
    public void finish()
            throws IOException
    {
        zstdOutputStream.close();
    }

    @Override
    public void resetState()
            throws IOException
    {
        finish();
        zstdOutputStream = new ZstdOutputStream(out);
    }
}
