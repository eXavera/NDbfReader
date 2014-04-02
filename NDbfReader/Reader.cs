using System;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Forward-only reader of dBASE table rows.
    /// </summary>
    public class Reader
    {
        private const byte END_OF_FILE = 0x1A;
        private const byte DELETED_ROW_FLAG = (byte)'*';

        private readonly Table _table;
        private Encoding _encoding;
        private bool _rowLoaded;
        private int _loadedRowCount = 0;
        private int _currentRowOffset = -1;

        /// <summary>
        /// Initializes a new instance from the specified table and encoding.
        /// </summary>
        /// <param name="table">The table from which rows will be read.</param>
        /// <param name="encoding">The encoding of the tables's rows.</param>
        /// <exception cref="ArgumentNullException"><paramref name="table"/> or <paramref name="encoding"/> is <c>null</c>.</exception>
        public Reader(Table table, Encoding encoding)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            if (encoding == null)
            {
                throw new ArgumentNullException("encoding");
            }

            _table = table;
            _encoding = encoding;
        }

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
        /// Moves the reader to the next row.
        /// </summary>
        /// <returns><c>true</c> if there are more rows; otherwise <c>false</c>.</returns>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public bool Read()
        {
            ThrowIfDisposed();

            _rowLoaded = ReadNextRow();

            return _rowLoaded;
        }

        /// <summary>
        /// Gets a <see cref="String"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="String"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="String"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual string GetString(string columnName)
        {
            return GetTypedValue<string>(columnName);
        }

        /// <summary>
        /// Gets a <see cref="String"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="String"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="String"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual string GetString(IColumn column)
        {
            return GetTypedValue<string>(column);
        }

        /// <summary>
        /// Gets a <see cref="Decimal"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="Decimal"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="Decimal"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual decimal? GetDecimal(string columnName)
        {
            return GetTypedValue<decimal?>(columnName);
        }

        /// <summary>
        /// Gets a <see cref="Decimal"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="Decimal"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="Decimal"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual decimal? GetDecimal(IColumn column)
        {
            return GetTypedValue<decimal?>(column);
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
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual DateTime? GetDate(string columnName)
        {
            return GetTypedValue<DateTime?>(columnName);
        }

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
        public virtual DateTime? GetDate(IColumn column)
        {
            return GetTypedValue<DateTime?>(column);
        }

        /// <summary>
        /// Gets a <see cref="Boolean"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="Boolean"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="Boolean"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual bool? GetBoolean(string columnName)
        {
            return GetTypedValue<bool?>(columnName);
        }


        /// <summary>
        /// Gets a <see cref="Boolean"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="Boolean"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="Boolean"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual bool? GetBoolean(IColumn column)
        {
            return GetTypedValue<bool?>(column);
        }

        /// <summary>
        /// Gets a <see cref="Int32"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        /// <returns>A <see cref="Int32"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// No column with this name was found.<br />
        /// -- or --<br />
        /// The column has different type then <see cref="Int32"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual int GetInt32(string columnName)
        {
            return GetTypedValue<int>(columnName);
        }

        /// <summary>
        /// Gets a <see cref="Int32"/> value of the specified column of the current row.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns>A <see cref="Int32"/> value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The column has different type then <see cref="Int32"/>.<br />
        /// -- or --<br />
        /// The column is from different table instance.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
        /// -- or --<br />
        /// The underlying stream is non-seekable and columns are read out of order.
        /// </exception>
        /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
        public virtual int GetInt32(IColumn column)
        {
            return GetTypedValue<int>(column);
        }

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
                throw new ArgumentNullException("columnName");
            }

            ValidateReaderState();

            var column = (Column)FindColumnByName(columnName);

            return column.LoadValueAsObject(LoadColumnBytes(column.Offset, column.Size), _encoding);
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
                throw new ArgumentNullException("column");
            }

            ValidateReaderState();
            CheckColumnBelongsToParentTable(column);

            var columnBase = (Column)column;

            return columnBase.LoadValueAsObject(LoadColumnBytes(columnBase.Offset, columnBase.Size), _encoding);
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if the parent table is disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            ParentTable.ThrowIfDisposed();
        }

        /// <summary>
        /// Gets the header of the parent table.
        /// </summary>
        protected Header Header
        {
            get
            {
                return ParentTable.Header;
            }
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
        protected T GetTypedValue<T>(string columnName)
        {
            if (columnName == null)
            {
                throw new ArgumentNullException("columnName");
            }

            ValidateReaderState();

            var typedColumn = FindColumnByName(columnName) as Column<T>;
            if (typedColumn == null)
            {
                throw new ArgumentOutOfRangeException("columnName", "The column's type does not match the method's return type.");
            }

            var valueBytes = LoadColumnBytes(typedColumn.Offset, typedColumn.Size);
            return typedColumn.LoadValue(valueBytes, _encoding);
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
        protected T GetTypedValue<T>(IColumn column)
        {
            if (column == null)
            {
                throw new ArgumentNullException("column");
            }

            ValidateReaderState();
            CheckColumnBelongsToParentTable(column);

            if (column.Type != typeof(T))
            {
                throw new ArgumentOutOfRangeException("column", "The column's type does not match the method's return type.");
            }

            var typedColumn = (Column<T>)column;
            var valueBytes = LoadColumnBytes(typedColumn.Offset, typedColumn.Size);
            return typedColumn.LoadValue(valueBytes, _encoding);
        }

        private BinaryReader BinaryReader
        {
            get
            {
                return ParentTable.BinaryReader;
            }
        }

        private IParentTable ParentTable
        {
            get
            {
                return (IParentTable)_table;
            }
        }

        private bool ReadNextRow()
        {
            if (_loadedRowCount >= Header.RowCount)
            {
                return false;
            }

            MoveToTheEndOfCurrentRow();

            if (SkipDeletedRows() == SkipDeletedRowsResult.EndOfFile)
            {
                return false;
            }

            _currentRowOffset = 0;

            return true;
        }

        private void MoveToTheEndOfCurrentRow()
        {
            if (_currentRowOffset >= 0)
            {
                BinaryReader.BaseStream.SeekForward(Header.RowSize - _currentRowOffset - 1);
            }
        }

        private SkipDeletedRowsResult SkipDeletedRows()
        {
            var isRowDeleted = false;

            do
            {
                var nextByte = BinaryReader.ReadByte();

                if (nextByte == END_OF_FILE)
                {
                    return SkipDeletedRowsResult.EndOfFile;
                }

                isRowDeleted = (nextByte == DELETED_ROW_FLAG);

                if (isRowDeleted)
                {
                    BinaryReader.BaseStream.SeekForward(Header.RowSize - 1);
                }

                _loadedRowCount += 1;
            }
            while (isRowDeleted);

            return SkipDeletedRowsResult.OK;
        }

        private byte[] LoadColumnBytes(int offset, int size)
        {
            var seek = offset - _currentRowOffset;

            if (seek < 0)
            {
                if (BinaryReader.BaseStream.CanSeek)
                {
                    BinaryReader.BaseStream.Seek(seek, SeekOrigin.Current);
                }
                else
                {
                    throw new InvalidOperationException("The underlying non-seekable stream does not allow reading the columns out of order.");
                }
            }
            else if (seek > 0)
            {
                BinaryReader.BaseStream.SeekForward(seek);
            }

            _currentRowOffset += (seek + size);

            return BinaryReader.ReadBytes(size);
        }

        private void ValidateReaderState()
        {
            ThrowIfDisposed();

            if (!_rowLoaded)
            {
                throw new InvalidOperationException("No row loaded. Call Read method first and check whether it returns true.");
            }
        }

        private void CheckColumnBelongsToParentTable(IColumn column)
        {
            if (!_table.Columns.Contains(column))
            {
                throw new ArgumentOutOfRangeException("column", "The column instance doesn't belong to this table.");
            }
        }

        private IColumn FindColumnByName(string columnName)
        {
            var column = Header.Columns.FirstOrDefault(c => c.Name == columnName);
            if (column == null)
            {
                throw ExceptionFactory.CreateArgumentOutOfRangeException("columnName", "Column {0} not found.", columnName);
            }

            return column;
        }

        private enum SkipDeletedRowsResult
        {
            OK,
            EndOfFile
        }
    }
}
