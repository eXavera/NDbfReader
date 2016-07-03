using System;
using System.Collections.Generic;

namespace NDbfReader
{
    /// <summary>
    /// Represents a header of the dBASE table.
    /// </summary>
    public class Header
    {
        /// <summary>
        /// Initializes a new instance with the specified rows size, rows count and columns.
        /// </summary>
        /// <param name="lastModified">The date the table was last modified.</param>
        /// <param name="rowCount">The number of rows in a table.</param>
        /// <param name="rowSize">The size of a row in bytes.</param>
        /// <param name="columns">The columns in the table.</param>
        /// <exception cref="ArgumentNullException"><paramref name="columns"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="rowSize"/> is &lt; 0 or <paramref name="rowCount"/> is &lt; 0.</exception>
        public Header(DateTime lastModified, int rowCount, int rowSize, IList<IColumn> columns)
        {
            if (rowCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(rowCount));
            }
            if (rowSize < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(rowSize));
            }
            if (columns == null)
            {
                throw new ArgumentNullException(nameof(columns));
            }

            LastModified = lastModified;
            RowCount = rowCount;
            RowSize = rowSize;
            Columns = new ColumnCollection(columns);
        }

        /// <summary>
        /// Gets the columns.
        /// </summary>
        public ColumnCollection Columns { get; }

        /// <summary>
        /// Gets the date the table was last modified.
        /// </summary>
        public DateTime LastModified { get; }

        /// <summary>
        /// Gets the number of rows in the table, including deleted ones.
        /// </summary>
        public int RowCount { get; }

        /// <summary>
        /// Gets the size of a row in bytes.
        /// </summary>
        public int RowSize { get; }
    }
}