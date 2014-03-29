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
        private readonly int _rowSize;
        private readonly int _rowCount;
        private readonly ReadOnlyCollection<IColumn> _columns;

        /// <summary>
        /// Initializes a new instance with the specified rows size, rows count and columns.
        /// </summary>
        /// <param name="columns">The columns in the table.</param>
        /// <param name="rowSize">The size of a row in bytes.</param>
        /// <param name="rowCount">The number of rows in a table.</param>
        /// <exception cref="ArgumentNullException"><paramref name="columns"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="rowSize"/> is &lt; 0 or <paramref name="rowCount"/> is &lt; 0.</exception>
        public Header(IList<IColumn> columns, int rowSize, int rowCount)
        {
            if (columns == null)
            {
                throw new ArgumentNullException("columns");
            }
            if (rowSize < 0)
            {
                throw new ArgumentOutOfRangeException("rowSize");
            }
            if (rowCount < 0)
            {
                throw new ArgumentOutOfRangeException("rowCount");
            }

            _columns = new ReadOnlyCollection<IColumn>(columns);
            _rowSize = rowSize;
            _rowCount = rowCount;
        }

        /// <summary>
        /// Gets the columns.
        /// </summary>
        public ReadOnlyCollection<IColumn> Columns
        {
            get { return _columns; }
        }

        /// <summary>
        /// Gets the size of a row in bytes. 
        /// </summary>
        public int RowSize
        {
            get { return _rowSize; }
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
    }
}
