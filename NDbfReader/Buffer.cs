using System;
using System.Collections.Generic;
using System.Linq;

namespace NDbfReader
{
    internal sealed class Buffer
    {
        private readonly IDictionary<Column, int> _bufferMap;

        public Buffer(IEnumerable<FillBufferInstruction> fillBufferInstructions, IDictionary<Column, int> bufferMap)
        {
            if (fillBufferInstructions == null)
            {
                throw new ArgumentNullException(nameof(fillBufferInstructions));
            }
            if (bufferMap == null)
            {
                throw new ArgumentNullException(nameof(bufferMap));
            }

            FillBufferInstructions = fillBufferInstructions;
            _bufferMap = bufferMap;

            int bufferSize = _bufferMap.Keys.Sum(column => column.Size);
            Data = new byte[bufferSize];
        }

        public byte[] Data { get; }

        public IEnumerable<FillBufferInstruction> FillBufferInstructions { get; }

        public Column FindColumnByName(string columnName)
        {
            if (string.IsNullOrEmpty(columnName))
            {
                throw new ArgumentNullException(nameof(columnName));
            }

            return _bufferMap.Keys.FirstOrDefault(c => c.Name == columnName);
        }

        public int GetBufferOffsetForColumn(Column column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            return _bufferMap[column];
        }

        public bool HasColumn(Column column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            return _bufferMap.ContainsKey(column);
        }
    }
}