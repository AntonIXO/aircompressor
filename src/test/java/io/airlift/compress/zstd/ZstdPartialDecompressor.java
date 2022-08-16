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

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.zstd.ZstdFrameDecompressor.DecompressState;

import java.nio.ByteBuffer;

import static java.lang.String.format;
import static java.util.Arrays.copyOfRange;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ZstdPartialDecompressor
        implements Decompressor
{
    private final ZstdFrameDecompressor decompressor = new ZstdFrameDecompressor();

    @Override
    public int decompress(final byte[] input, final int inputOffset, final int inputLength, final byte[] output, final int outputOffset, final int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        DecompressState decompressState = DecompressState.INITIAL_DECOMPRESS_STATE;
        assertEquals(decompressState.getInputRequired(), 0);
        assertEquals(decompressState.getRequestedOutputSize(), 0);
        assertEquals(decompressState.getInputConsumed(), 0);
        assertEquals(decompressState.getOutputBufferUsed(), 0);

        int inputPosition = inputOffset;
        final int inputLimit = inputOffset + inputLength;
        int outputPosition = outputOffset;
        final int outputLimit = outputOffset + maxOutputLength;

        while (inputPosition < inputLimit || decompressState.getRequestedOutputSize() > 0) {
            assertTrue(decompressState.getInputRequired() <= inputLimit - inputPosition);
            if (outputPosition + decompressState.getRequestedOutputSize() > outputLimit) {
                throw new IllegalArgumentException("Output buffer too small");
            }

            // for testing, we always send the minimum number of requested bytes
            byte[] inputChunk = copyOfRange(input, inputPosition, inputPosition + decompressState.getInputRequired());

            // for testing, we use two reads for larger buffers
            byte[] outputBuffer = new byte[0];
            if (decompressState.getRequestedOutputSize() > 0) {
                outputBuffer = new byte[decompressState.getRequestedOutputSize() > 500 ? decompressState.getRequestedOutputSize() - 457 : decompressState.getRequestedOutputSize()];
            }

            decompressState = decompressor.partialDecompress(
                    decompressState,
                    inputChunk,
                    ARRAY_BYTE_BASE_OFFSET,
                    inputChunk.length + ARRAY_BYTE_BASE_OFFSET,
                    outputBuffer,
                    0,
                    outputBuffer.length);

            // copy output chunk to output
            int outputBufferUsed = decompressState.getOutputBufferUsed();
            if (outputBufferUsed > 0) {
                assertTrue(outputPosition + outputBufferUsed <= outputLimit);
                System.arraycopy(outputBuffer, 0, output, outputPosition, outputBufferUsed);
                outputPosition += outputBufferUsed;
            }

            assertTrue(decompressState.getInputConsumed() <= inputChunk.length);
            inputPosition += decompressState.getInputConsumed();
        }
        return outputPosition - outputOffset;
    }

    @Override
    public void decompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
            throws MalformedInputException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
