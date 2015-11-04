using System;
using System.Collections.Generic;

namespace NDbfReader
{
    internal sealed class BufferBuilder
    {
        private readonly Dictionary<Column, int> _bufferMap;
        private readonly List<FillBufferInstruction> _instructions;
        private int _bufferOffset;
        private FillBufferInstruction _uncommittedInstruction;

        public BufferBuilder()
        {
            _instructions = new List<FillBufferInstruction>();
            _bufferMap = new Dictionary<Column, int>();

            _bufferOffset = 0;
        }

        public void AddColumn(Column column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            _bufferMap.Add(column, _bufferOffset);

            FillBufferInstruction newInstruction = FillBufferInstruction.Read(column.Size, _bufferOffset);
            if (_uncommittedInstruction == null)
            {
                _uncommittedInstruction = newInstruction;
            }
            else
            {
                if (_uncommittedInstruction.ShouldSkip)
                {
                    CommitPendingInstruction();
                    _uncommittedInstruction = newInstruction;
                }
                else
                {
                    _uncommittedInstruction = _uncommittedInstruction.Merge(newInstruction);
                }
            }

            _bufferOffset += column.Size;
        }

        public void AddHole(int rowOffset, int count)
        {
            if (rowOffset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(rowOffset));
            }
            if (count <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            FillBufferInstruction newInstruction = FillBufferInstruction.Skip(count);
            if (_uncommittedInstruction == null)
            {
                _uncommittedInstruction = newInstruction;
            }
            else
            {
                if (_uncommittedInstruction.ShouldSkip)
                {
                    _uncommittedInstruction = _uncommittedInstruction.Merge(newInstruction);
                }
                else
                {
                    CommitPendingInstruction();
                    _uncommittedInstruction = newInstruction;
                }
            }
        }

        public Buffer Build()
        {
            CommitPendingInstruction();

            return new Buffer(_instructions, _bufferMap);
        }

        private void CommitPendingInstruction()
        {
            if (_uncommittedInstruction != null)
            {
                _instructions.Add(_uncommittedInstruction);
            }
        }
    }
}