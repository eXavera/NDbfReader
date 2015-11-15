using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        private readonly Buffer _buffer;
        private readonly Table _table;
        private Encoding _encoding;
        private byte[] _helperBufferForSeeking;
        private int _loadedRowCount = 0;
        private bool _rowLoaded;

        /// <summary>
        /// Initializes a new instance from the specified table and encoding.
        /// </summary>
        /// <param name="table">The table from which rows will be loaded.</param>
        /// <param name="columnsToLoad">The list of columns to load.</param>
        /// <param name="encoding">The encoding of the tables's rows.</param>
        /// <exception cref="ArgumentNullException"><paramref name="table"/> or <paramref name="encoding"/> is <c>null</c> or <paramref name="columnsToLoad"/> is <c>null</c>.</exception>
        public Reader(Table table, Encoding encoding, ICollection<IColumn> columnsToLoad)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }
            if (encoding == null)
            {
                throw new ArgumentNullException(nameof(encoding));
            }
            if (columnsToLoad == null)
            {
                throw new ArgumentNullException(nameof(columnsToLoad));
            }

            _table = table;
            _encoding = encoding;
            _buffer = CreateBuffer(columnsToLoad.Cast<Column>(), Header.RowSize);
        }

        /// <summary>
        /// Gets the encoding used to decode a row's content.
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

        private byte[] HelperBufferForSeeking
        {
            get
            {
                if (_helperBufferForSeeking == null)
                {
                    _helperBufferForSeeking = new byte[Math.Min(Header.RowSize, 4096)];
                }
                return _helperBufferForSeeking;
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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual bool? GetBoolean(string columnName) => GetValue<bool?>(columnName);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual bool? GetBoolean(IColumn column) => GetValue<bool?>(column);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual DateTime? GetDate(string columnName) => GetValue<DateTime?>(columnName);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual DateTime? GetDate(IColumn column) => GetValue<DateTime?>(column);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual decimal? GetDecimal(string columnName) => GetValue<decimal?>(columnName);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual decimal? GetDecimal(IColumn column) => GetValue<decimal?>(column);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual int GetInt32(string columnName) => GetValue<int>(columnName);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual int GetInt32(IColumn column) => GetValue<int>(column);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual string GetString(string columnName) => GetValue<string>(columnName);

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual string GetString(IColumn column) => GetValue<string>(column);

        /// <summary>
        /// Gets a value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual object GetValue(string columnName)
        {
            if (columnName == null)
            {
                throw new ArgumentNullException(nameof(columnName));
            }
            ValidateReaderState();

            var column = (Column)FindColumnByName(columnName);
            return column.LoadValueAsObject(_buffer.Data, _buffer.GetBufferOffsetForColumn(column), _encoding);
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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual object GetValue(IColumn column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }
            ValidateReaderState();

            var columnBase = (Column)column;
            CheckColumnExists(columnBase);

            return columnBase.LoadValueAsObject(_buffer.Data, _buffer.GetBufferOffsetForColumn(columnBase), _encoding);
        }

        /// <summary>
        /// Moves the reader to the next row.
        /// </summary>
        /// <returns><c>true</c> if there are more rows; otherwise <c>false</c>.</returns>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public bool Read()
        {
            ThrowIfDisposed();

            if (_loadedRowCount >= Header.RowCount)
            {
                return _rowLoaded = false;
            }

            var isRowDeleted = false;
            do
            {
                Stream.Read(_buffer.Data, 0, 1);
                byte nextByte = _buffer.Data[0];
                if (nextByte == END_OF_FILE)
                {
                    return _rowLoaded = false;
                }

                isRowDeleted = (nextByte == DELETED_ROW_FLAG);
                if (isRowDeleted)
                {
                    SkipStreamBytes(Header.RowSize - 1);
                }

                _loadedRowCount += 1;
            }
            while (isRowDeleted);

            for(int i = 0; i < _buffer.FillBufferInstructions.Count; i++)
            {
                FillBufferInstruction instruction = _buffer.FillBufferInstructions[i];
                if (instruction.ShouldSkip)
                {
                    SkipStreamBytes(instruction.Count);
                }
                else
                {
                    Stream.Read(_buffer.Data, instruction.BufferOffset, instruction.Count);
                }
            }

            return _rowLoaded = true;
        }

        /// <summary>
        /// Moves the reader to the next row.
        /// </summary>
        /// <returns><c>true</c> if there are more rows; otherwise <c>false</c>.</returns>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public async Task<bool> ReadAsync()
        {
            ThrowIfDisposed();

            if (_loadedRowCount >= Header.RowCount)
            {
                return _rowLoaded = false;
            }

            var isRowDeleted = false;
            do
            {
                await Stream.ReadAsync(_buffer.Data, 0, 1).ConfigureAwait(false);
                byte nextByte = _buffer.Data[0];
                if (nextByte == END_OF_FILE)
                {
                    return _rowLoaded = false;
                }

                isRowDeleted = (nextByte == DELETED_ROW_FLAG);
                if (isRowDeleted)
                {
                    await SkipStreamBytesAsync(Header.RowSize - 1).ConfigureAwait(false);
                }

                _loadedRowCount += 1;
            }
            while (isRowDeleted);

            for (int i = 0; i < _buffer.FillBufferInstructions.Count; i++)
            {
                FillBufferInstruction instruction = _buffer.FillBufferInstructions[i];
                if (instruction.ShouldSkip)
                {
                    await SkipStreamBytesAsync(instruction.Count).ConfigureAwait(false);
                }
                else
                {
                    await Stream.ReadAsync(_buffer.Data, instruction.BufferOffset, instruction.Count).ConfigureAwait(false);
                }
            }

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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
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
            return typedColumn.LoadValue(_buffer.Data, _buffer.GetBufferOffsetForColumn(typedColumn), _encoding);
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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
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

            return typedColumn.LoadValue(_buffer.Data, _buffer.GetBufferOffsetForColumn(typedColumn), _encoding);
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if the parent table is disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            ParentTable.ThrowIfDisposed();
        }

        private static Buffer CreateBuffer(IEnumerable<Column> columnsToLoad, int rowSize)
        {
            var builder = new BufferBuilder();

            Column prevColumn = null;
            IEnumerable<Column> orderedColumns = columnsToLoad.Cast<Column>().OrderBy(c => c.Offset).ToArray();
            foreach (Column column in orderedColumns)
            {
                if (prevColumn == null)
                {
                    if (column.Offset > 0)
                    {
                        builder.AddHole(0, column.Offset);
                    }
                    builder.AddColumn(column);
                }
                else
                {
                    int prevColumnEndOffset = prevColumn.Offset + prevColumn.Size;
                    int holeSize = column.Offset - prevColumnEndOffset;
                    if (holeSize > 0)
                    {
                        builder.AddHole(prevColumnEndOffset, holeSize);
                    }
                    builder.AddColumn(column);
                }

                prevColumn = column;
            }

            Column lastColumn = orderedColumns.Last();
            int lastColumnEndOffset = lastColumn.Offset + lastColumn.Size;
            int lastHoleSize = (rowSize - (lastColumnEndOffset + 1));
            if (lastHoleSize > 0)
            {
                builder.AddHole(lastColumnEndOffset, lastHoleSize);
            }

            return builder.Build();
        }

        private IColumn FindColumnByName(string columnName)
        {
            IColumn column = _buffer.FindColumnByName(columnName);
            if (column == null)
            {
                throw new ArgumentOutOfRangeException(nameof(columnName), $"Column {columnName} not found.");
            }
            return column;
        }

        private void CheckColumnExists(Column column)
        {
            if (!_buffer.HasColumn(column))
            {
                throw new ArgumentOutOfRangeException(nameof(column), "The column instance not found.");
            }
        }

        private void SkipStreamBytes(int offset)
        {
            if (Stream.CanSeek)
            {
                Stream.Seek(offset, SeekOrigin.Current);
            }
            else
            {
                byte[] buffer = HelperBufferForSeeking;
                int bytesToRead = offset;
                while (bytesToRead > 0)
                {
                    int readBytes = Stream.Read(buffer, 0, bytesToRead > buffer.Length ? buffer.Length : bytesToRead);
                    if (readBytes == 0)
                    {
                        break;
                    }

                    bytesToRead -= readBytes;
                }
            }
        }

        private async Task SkipStreamBytesAsync(int offset)
        {
            if (Stream.CanSeek)
            {
                Stream.Seek(offset, SeekOrigin.Current);
            }
            else
            {
                byte[] buffer = HelperBufferForSeeking;
                int bytesToRead = offset;
                while (bytesToRead > 0)
                {
                    int readBytes = await Stream.ReadAsync(buffer, 0, bytesToRead > buffer.Length ? buffer.Length : bytesToRead).ConfigureAwait(false);
                    if (readBytes == 0)
                    {
                        break;
                    }

                    bytesToRead -= readBytes;
                }
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