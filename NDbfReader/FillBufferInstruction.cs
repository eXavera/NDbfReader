using System;

namespace NDbfReader
{
    internal sealed class FillBufferInstruction
    {
        private const int EMPTY_BUFFER_OFFSET = -1;

        private FillBufferInstruction(int count, int bufferOffset)
        {
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }
            if (bufferOffset < EMPTY_BUFFER_OFFSET)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferOffset));
            }

            Count = count;
            BufferOffset = bufferOffset;
        }

        public int BufferOffset { get; }

        public int Count { get; }

        public bool ShouldSkip => BufferOffset == EMPTY_BUFFER_OFFSET;

        public static FillBufferInstruction Read(int count, int bufferOffset)
        {
            return new FillBufferInstruction(count, bufferOffset);
        }

        public static FillBufferInstruction Skip(int count)
        {
            return new FillBufferInstruction(count, EMPTY_BUFFER_OFFSET);
        }

        public FillBufferInstruction Merge(FillBufferInstruction other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            return new FillBufferInstruction(Count + other.Count, BufferOffset);
        }
    }
}