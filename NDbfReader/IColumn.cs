using System;

namespace NDbfReader
{
    /// <summary>
    /// Represents a dBASE column.
    /// </summary>
    public interface IColumn
    {
        /// <summary>
        /// Gets the column name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the column size in bytes.
        /// </summary>
        int Size { get; }

        /// <summary>
        /// Gets the <c>CLR</c> type of a column value.
        /// </summary>
        Type Type { get; }
    }
}