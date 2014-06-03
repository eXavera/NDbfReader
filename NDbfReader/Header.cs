using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace NDbfReader
{
    /// <summary>
    /// Represents a header of the dBASE table.
    /// </summary>
    public class Header
    {
        private readonly DateTime _lastModified;
        private readonly int _rowCount;
        private readonly int _rowSize;
        private readonly ReadOnlyCollection<IColumn> _columns;

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
                throw new ArgumentOutOfRangeException("rowCount");
            }
            if (rowSize < 0)
            {
                throw new ArgumentOutOfRangeException("rowSize");
            }
            if (columns == null)
            {
                throw new ArgumentNullException("columns");
            }

            _lastModified = lastModified;
            _rowCount = rowCount;
            _rowSize = rowSize;
            _columns = new ReadOnlyCollection<IColumn>(columns);
        }

        /// <summary>
        /// Gets a date the table was last modified.
        /// </summary>
        public DateTime LastModified
        {
            get
            {
                return _lastModified;
            }
        }

        /// <summary>
        /// Gets the number of rows in the table, including deleted ones.
        /// </summary>
        public int RowCount
        {
            get
            {
                return _rowCount;
            }
        }

        /// <summary>
        /// Gets the size of a row in bytes. 
        /// </summary>
        public int RowSize
        {
            get { return _rowSize; }
        }

        /// <summary>
        /// Gets the columns.
        /// </summary>
        public ReadOnlyCollection<IColumn> Columns
        {
            get { return _columns; }
        }

    }
}
