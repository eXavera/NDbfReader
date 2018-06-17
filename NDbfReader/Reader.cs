using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NDbfReader
{
    /// <summary>
    /// Forward-only reader of dBASE table rows.
    /// </summary>
    public class Reader
    {
        private const byte DELETED_ROW_FLAG = (byte)'*';
        private const byte END_OF_FILE = 0x1A;
        private const int MAX_BUFFER_SIZE = 4096;
        private const int MAX_ROWS_IN_BUFFER = 3;

        private readonly byte[] _buffer;
        private readonly int _rowSize;
        private readonly Table _table;
        private int _bufferOffset;
        private Encoding _encoding;
        private int _loadedRowCount;
        private bool _rowLoaded;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="table">The table from which rows will be loaded.</param>
        /// <param name="encoding">The encoding.</param>
        /// <exception cref="ArgumentNullException"><paramref name="table"/> is <c>null</c> or <paramref name="encoding"/> is <c>null</c>.</exception>
        public Reader(Table table, Encoding encoding)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }
            if (encoding == null)
            {
                throw new ArgumentNullException(nameof(encoding));
            }

            _table = table;
            _encoding = encoding;
            _rowSize = Header.RowSize;

            int bufferSize = 0;
            if (_rowSize >= MAX_BUFFER_SIZE)
            {
                bufferSize = _rowSize;
            }
            else
            {
                int rowsInBuffer = Math.Min(MAX_BUFFER_SIZE / _rowSize, MAX_ROWS_IN_BUFFER);
                bufferSize = rowsInBuffer * _rowSize;
            }

            _buffer = new byte[bufferSize];
        }

        /// <summary>
        /// Gets the encoding used to parse the table content.
        /// </summary>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public Encoding Encoding
        {
            get
            {
                ThrowIfDisposed();

                return _encoding;
            }
        }

        /// <summary>
        /// Gets the header of the parent table.
        /// </summary>
        protected Header Header => ParentTable.Header;

        private Stream Stream => ParentTable.Stream;

        /// <summary>
        /// Gets the parent table.
        /// </summary>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public Table Table
        {
            get
            {
                ThrowIfDisposed();

                return _table;
            }
        }

        private IParentTable ParentTable => _table;

        /// <summary>
        /// Gets a <see cref="bool"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="bool"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="bool"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public bool? GetBoolean(string columnName) => GetValue<bool?>(columnName);

        /// <summary>
        /// Gets a <see cref="bool"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="bool"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="bool"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public bool? GetBoolean(IColumn column) => GetValue<bool?>(column);

        /// <summary>
        /// Gets raw bytes of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the raw bytes of the specified column starting at the specified <paramref name="offset"/>.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin storing the raw bytes of the specified column.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="column"/> is <c>null</c>.<br />
        /// -- or --<br />
        /// <paramref name="buffer"/> is <c>null</c>.<br />
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="column"/> is from different table instance.<br />
        /// -- or --<br />
        /// <paramref name="offset"/> &lt; 0.<br />
        /// -- or --<br />
        /// <paramref name="offset"/> is larger then the buffer length.<br />
        /// </exception>
        /// <exception cref="ArgumentException"><paramref name="buffer"/> is too small. The buffer length has to be at least column size + <paramref name="offset"/>.</exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public void GetBytes(IColumn column, byte[] buffer, int offset = 0)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }
            if (offset < 0 || offset >= buffer.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            ValidateReaderState();

            var columnBase = (Column)column;
            CheckColumnExists(columnBase);
            ValidateBufferSize(buffer, offset, column);

            CopyColumnBytes(columnBase, buffer, offset);
        }

        /// <summary>
        /// Gets raw bytes of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the raw bytes of the specified column starting at the specified <paramref name="offset"/>.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin storing the raw bytes of the specified column.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="columnName"/> is <c>null</c>.<br />
        /// -- or --<br />
        /// <paramref name="buffer"/> is <c>null</c>.<br />
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="offset"/> &lt; 0.<br />
        /// -- or --<br />
        /// <paramref name="offset"/> is larger then the buffer length.<br />
        /// </exception>
        /// <exception cref="ArgumentException"><paramref name="buffer"/> is too small. The buffer length has to be at least column size + <paramref name="offset"/>.</exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public void GetBytes(string columnName, byte[] buffer, int offset = 0)
        {
            if (columnName == null)
            {
                throw new ArgumentNullException(nameof(columnName));
            }
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }
            if (offset < 0 || offset >= buffer.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            ValidateReaderState();

            var column = (Column)FindColumnByName(columnName);
            ValidateBufferSize(buffer, offset, column);

            CopyColumnBytes(column, buffer, offset);
        }

        /// <summary>
        /// Gets a <see cref="DateTime"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="DateTime"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="DateTime"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public DateTime? GetDateTime(string columnName) => GetValue<DateTime?>(columnName);

        /// <summary>
        /// Gets a <see cref="DateTime"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="DateTime"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="DateTime"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        [Obsolete("This method has been renamed to " + nameof(GetDateTime))]
        public DateTime? GetDate(string columnName) => GetDateTime(columnName);

        /// <summary>
        /// Gets a <see cref="DateTime"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="DateTime"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="DateTime"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public DateTime? GetDateTime(IColumn column) => GetValue<DateTime?>(column);

        /// <summary>
        /// Gets a <see cref="DateTime"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="DateTime"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="DateTime"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        [Obsolete("This method has been renamed to " + nameof(GetDateTime))]
        public DateTime? GetDate(IColumn column) => GetDateTime(column);

        /// <summary>
        /// Gets a <see cref="decimal"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="decimal"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="decimal"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public decimal? GetDecimal(string columnName) => GetValue<decimal?>(columnName);

        /// <summary>
        /// Gets a <see cref="decimal"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="decimal"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="decimal"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public decimal? GetDecimal(IColumn column) => GetValue<decimal?>(column);

        /// <summary>
        /// Gets a <see cref="int"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="int"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="int"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public int GetInt32(string columnName) => GetValue<int>(columnName);

        /// <summary>
        /// Gets a <see cref="int"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="int"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="int"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public int GetInt32(IColumn column) => GetValue<int>(column);

        /// <summary>
        /// Gets a <see cref="string"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="string"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="string"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public string GetString(string columnName) => GetValue<string>(columnName);

        /// <summary>
        /// Gets a <see cref="string"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="string"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="string"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public string GetString(IColumn column) => GetValue<string>(column);

        /// <summary>
        /// Gets a value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public object GetValue(string columnName)
        {
            if (columnName == null)
            {
                throw new ArgumentNullException(nameof(columnName));
            }
            ValidateReaderState();

            var column = (Column)FindColumnByName(columnName);
            return column.LoadValueAsObject(_buffer, GetColumnOffsetInBuffer(column), _encoding);
        }

        /// <summary>
        /// Gets a value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public object GetValue(IColumn column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }
            ValidateReaderState();

            var columnBase = (Column)column;
            CheckColumnExists(columnBase);

            return columnBase.LoadValueAsObject(_buffer, GetColumnOffsetInBuffer(columnBase), _encoding);
        }

        /// <summary>
        /// Moves the reader to the next row.
        /// </summary>
        /// <returns><c>true</c> if there are more rows; otherwise <c>false</c>.</returns>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public bool Read()
        {
            ThrowIfDisposed();

            do
            {
                if (_loadedRowCount >= Header.RowCount)
                {
                    return _rowLoaded = false;
                }

                int newBufferOffset = _bufferOffset + _rowSize;
                if (newBufferOffset >= _buffer.Length || _loadedRowCount == 0)
                {
                    Stream.ReadBlock(_buffer, 0, _buffer.Length);
                    newBufferOffset = 0;
                }

                _bufferOffset = newBufferOffset;
                _loadedRowCount += 1;
            }
            while (_buffer[_bufferOffset] == DELETED_ROW_FLAG);

            return _rowLoaded = true;
        }

        /// <summary>
        /// Moves the reader to the next row.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns><c>true</c> if there are more rows; otherwise <c>false</c>.</returns>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public async Task<bool> ReadAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ThrowIfDisposed();

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_loadedRowCount >= Header.RowCount)
                {
                    return _rowLoaded = false;
                }

                int newBufferOffset = _bufferOffset + _rowSize;
                if (newBufferOffset >= _buffer.Length || _loadedRowCount == 0)
                {
                    await Stream.ReadBlockAsync(_buffer, 0, _buffer.Length, cancellationToken).ConfigureAwait(false);
                    newBufferOffset = 0;
                }

                _bufferOffset = newBufferOffset;
                _loadedRowCount += 1;
            }
            while (_buffer[_bufferOffset] == DELETED_ROW_FLAG);

            return _rowLoaded = true;
        }

        /// <summary>
        /// Gets a value of the specified column of the current row.
        /// </summary>
        /// <typeparam name="T">The column type.</typeparam>
        /// <param name="columnName">The column name.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        protected T GetValue<T>(string columnName)
        {
            if (columnName == null)
            {
                throw new ArgumentNullException(nameof(columnName));
            }
            ValidateReaderState();

            var typedColumn = FindColumnByName(columnName) as Column<T>;
            if (typedColumn == null)
            {
                throw new ArgumentOutOfRangeException(nameof(columnName), "The column's type does not match the method's return type.");
            }
            return typedColumn.LoadValue(_buffer, GetColumnOffsetInBuffer(typedColumn), _encoding);
        }

        /// <summary>
        /// Gets a value of the specified column of the current row.
        /// </summary>
        /// <typeparam name="T">The column type.</typeparam>
        /// <param name="column">The column.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.</exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        protected T GetValue<T>(IColumn column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }
            ValidateReaderState();
            if (column.Type != typeof(T))
            {
                throw new ArgumentOutOfRangeException(nameof(column), "The column's type does not match the method's return type.");
            }

            var typedColumn = (Column<T>)column;
            CheckColumnExists(typedColumn);

            return typedColumn.LoadValue(_buffer, GetColumnOffsetInBuffer(typedColumn), _encoding);
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if the parent table is disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            ParentTable.ThrowIfDisposed();
        }

        private static void ValidateBufferSize(byte[] buffer, int offset, IColumn column)
        {
            if ((buffer.Length - offset) < column.Size)
            {
                throw new ArgumentException($"The buffer is too small. Increase the capacity to at least {column.Size + 1} bytes.", nameof(buffer));
            }
        }

        private void CopyColumnBytes(Column column, byte[] destBuffer, int destOffset)
        {
            Array.Copy(_buffer, GetColumnOffsetInBuffer(column), destBuffer, destOffset, column.Size);
        }

        private IColumn FindColumnByName(string columnName)
        {
            IColumn column = Header.Columns.FindByName(columnName);
            if (column == null)
            {
                throw new ArgumentOutOfRangeException(nameof(columnName), $"Column {columnName} not found.");
            }
            return column;
        }

        private int GetColumnOffsetInBuffer(Column column)
        {
            return _bufferOffset + column.Offset + 1;
        }

        private void CheckColumnExists(Column column)
        {
            if (!Header.Columns.Contains(column))
            {
                throw new ArgumentOutOfRangeException(nameof(column), "The column instance not found.");
            }
        }

        private void ValidateReaderState()
        {
            ThrowIfDisposed();

            if (!_rowLoaded)
            {
                throw new InvalidOperationException($"No row is loaded. Call {nameof(Read)} method first and check whether it returns true.");
            }
        }
    }
}