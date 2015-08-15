using System;
using System.IO;

namespace NDbfReader
{
    /// <summary>
    /// Stream extensions.
    /// </summary>
    public static class StreamExtensions
    {
        private const int MAX_BUFFER_SIZE = 255;
        private const int READ_BY_SINGLE_BYTE_OFFSET_SIZE_THRESHOLD = 3;

        /// <summary>
        /// Moves the position forward within the specified stream. Supports also non seekable streams.
        /// </summary>
        /// <param name="stream">The stream within the position should be moved.</param>
        /// <param name="offset">The byte offset relative to the current position within the stream.</param>
        /// <exception cref="ArgumentNullException"> <paramref name="stream"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is negative.</exception>
        public static void SeekForward(this Stream stream, int offset)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            if (stream.CanSeek)
            {
                stream.Seek(offset, SeekOrigin.Current);
            }
            else
            {
                SeekForwardByRead(stream, offset);
            }
        }

        private static void SeekForwardByRead(Stream stream, int offset)
        {
            if (offset <= READ_BY_SINGLE_BYTE_OFFSET_SIZE_THRESHOLD)
            {
                for (int i = 0; i < offset; i++)
                {
                    stream.ReadByte();
                }
            }
            else
            {
                int bufferSize = Math.Min(MAX_BUFFER_SIZE, offset);
                var buffer = new byte[bufferSize];
                int bytesToRead = offset;

                while (bytesToRead > 0)
                {
                    int readBytes = stream.Read(buffer, 0, bytesToRead > bufferSize ? bufferSize : bytesToRead);
                    if (readBytes == 0)
                    {
                        break;
                    }

                    bytesToRead -= readBytes;
                }
            }
        }
    }
}