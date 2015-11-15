using System;
using System.Collections.Generic;
using System.Linq;

namespace NDbfReader
{
    internal sealed class Buffer
    {
        private readonly IDictionary<Column, int> _bufferMap;
        private readonly IDictionary<string, Column> _columnsMap;

        public Buffer(IList<FillBufferInstruction> fillBufferInstructions, IDictionary<Column, int> bufferMap)
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
            _columnsMap = _bufferMap.Keys.ToDictionary(p => p.Name);

            int bufferSize = _bufferMap.Keys.Sum(column => column.Size);
            Data = new byte[bufferSize];
        }

        public byte[] Data { get; }

        public IList<FillBufferInstruction> FillBufferInstructions { get; }

        public Column FindColumnByName(string columnName)
        {
            if (string.IsNullOrEmpty(columnName))
            {
                throw new ArgumentNullException(nameof(columnName));
            }

            if (_columnsMap.ContainsKey(columnName))
            {
                return _columnsMap[columnName];
            }
            return null;
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