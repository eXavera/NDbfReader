using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NDbfReader
{
    /// <summary>
    /// Represents a loader of dBASE header used by <see cref="Table"/>.
    /// </summary>
    public class HeaderLoader
    {
        private const int COLUMN_DESCRIPTOR_SIZE = 32;
        private const int COLUMN_NAME_LENGTH = 11;
        private const int COLUMN_NAME_OFFSET = 0;
        private const int COLUMN_SIZE_OFFSET = 16;
        private const int COLUMN_TYPE_OFFSET = 11;
        private const int DAY_OFFSET = 3;
        private const byte FILE_DESCRIPTOR_TERMINATOR = 0x0D;
        private const int HEADER_SIZE = 32;
        private const int HEADER_SIZE_OFFSET = 8;
        private const int MONTH_OFFSET = 2;
        private const int ROW_COUNT_OFFSET = 4;
        private const int ROW_SIZE_OFFSET = 10;
        private const int YEAR_OFFSET = 1;

        private static readonly HeaderLoader _default = new HeaderLoader();

        /// <summary>
        /// Gets the default loader.
        /// </summary>
        public static HeaderLoader Default => _default;

        /// <summary>
        /// Loads a header from the specified stream.
        /// </summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>A header loaded from the specified stream.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c>.</exception>
        public virtual Header Load(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // optimization: be one byte ahead when loading columns => only one I/O read per column
            var buffer = new byte[HEADER_SIZE + 1];
            int totalReadBytes = stream.ReadBlock(buffer, 0, buffer.Length);

            BasicProperties headerProperties = ParseBasicProperties(buffer);

            LoadColumnsResult loadColumnsResult = LoadColumns(stream, buffer.Last());
            totalReadBytes += loadColumnsResult.ReadBytes;

            int bytesToSkip = headerProperties.HeaderSize - totalReadBytes;
            if (bytesToSkip > 0)
            {
                // move to the end of the header
                if (stream.CanSeek)
                {
                    stream.Seek(bytesToSkip, SeekOrigin.Current);
                }
                else
                {
                    while (bytesToSkip > 0)
                    {
                        bytesToSkip -= stream.ReadBlock(buffer, 0, Math.Min(buffer.Length, bytesToSkip));
                    }
                }
            }

            return CreateHeader(headerProperties, loadColumnsResult.Columns);
        }

        /// <summary>
        /// Loads a header from the specified stream.
        /// </summary>
        /// <param name="stream">The input stream.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A header loaded from the specified stream.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c>.</exception>
        public virtual async Task<Header> LoadAsync(Stream stream, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // optimization: be one byte ahead when loading columns => only one I/O read per column
            var buffer = new byte[HEADER_SIZE + 1];
            int totalReadBytes = await stream.ReadBlockAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);

            BasicProperties headerProperties = ParseBasicProperties(buffer);

            LoadColumnsResult loadColumnsResult = await LoadColumnsAsync(stream, buffer.Last(), cancellationToken).ConfigureAwait(false);
            totalReadBytes += loadColumnsResult.ReadBytes;

            int bytesToSkip = headerProperties.HeaderSize - totalReadBytes;
            if (bytesToSkip > 0)
            {
                // move to the end of the header
                if (stream.CanSeek)
                {
                    stream.Seek(bytesToSkip, SeekOrigin.Current);
                }
                else
                {
                    // async read is more expensive then sync read so we use bigger buffer to reduce the number of I/O reads
                    const int MAX_SKIP_BUFFER_SIZE = 512;

                    byte[] skipBuffer = buffer;
                    if (bytesToSkip > buffer.Length)
                    {
                        skipBuffer = new byte[Math.Min(bytesToSkip, MAX_SKIP_BUFFER_SIZE)];
                    }

                    while (bytesToSkip > 0)
                    {
                        bytesToSkip -= await stream.ReadBlockAsync(skipBuffer, 0, Math.Min(skipBuffer.Length, bytesToSkip), cancellationToken).ConfigureAwait(false);
                    }
                }
            }

            return CreateHeader(headerProperties, loadColumnsResult.Columns);
        }

        /// <summary>
        /// Creates a column based on the specified properties.
        /// </summary>
        /// <param name="size">The column size in bytes.</param>
        /// <param name="type">The column native type.</param>
        /// <param name="name">The column name.</param>
        /// <param name="columnOffset">The column offset in a row.</param>
        /// <returns>A column instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="size"/> or <paramref name="columnOffset"/> is &lt; 0.</exception>
        protected virtual Column CreateColumn(byte size, byte type, string name, int columnOffset)
        {
            if (size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (columnOffset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(columnOffset));
            }

            switch (type)
            {
                case NativeColumnType.Char:
                    return new StringColumn(name, columnOffset, size);

                case NativeColumnType.Date:
                    return new DateTimeColumn(name, columnOffset, size);

                case NativeColumnType.FoxProDateTime:
                    return new FoxProDateTimeColumn(name, columnOffset, size);

                case NativeColumnType.Long:
                    return new Int32Column(name, columnOffset, size);

                case NativeColumnType.Logical:
                    return new BooleanColumn(name, columnOffset, size);

                case NativeColumnType.Numeric:
                case NativeColumnType.Float:
                    return new DecimalColumn(name, columnOffset, size);

                default:
                    return new RawColumn(name, columnOffset, size, type);
            }
        }

        /// <summary>
        /// Creates a header instance.
        /// </summary>
        /// <param name="properties">The header properties.</param>
        /// <param name="columns">The loaded columns.</param>
        /// <returns>A header instance.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="properties"/> is <c>null</c> or <paramref name="columns"/> is <c>null</c>.
        /// </exception>
        protected virtual Header CreateHeader(BasicProperties properties, IList<IColumn> columns)
        {
            if(properties == null)
            {
                throw new ArgumentNullException(nameof(properties));
            }
            if(columns == null)
            {
                throw new ArgumentNullException(nameof(columns));
            }

            return new Header(properties.LastModified, properties.RowCount, properties.RowSize, columns);
        }

        /// <summary>
        /// Parses the header properties from the specified buffer.
        /// </summary>
        /// <param name="buffer">The header bytes.</param>
        /// <returns>A <see cref="BasicProperties"/> instance.</returns>
        protected virtual BasicProperties ParseBasicProperties(byte[] buffer)
        {
            DateTime lastModified = ParseLastModifiedDate(buffer);
            int rowCount = BitConverter.ToInt32(buffer, ROW_COUNT_OFFSET);
            short rowSize = BitConverter.ToInt16(buffer, ROW_SIZE_OFFSET);
            short headerSize = BitConverter.ToInt16(buffer, HEADER_SIZE_OFFSET);

            return new BasicProperties(lastModified, rowCount, rowSize, headerSize);
        }

        private static DateTime ParseLastModifiedDate(byte[] buffer)
        {
            byte year = buffer[YEAR_OFFSET];
            byte month = buffer[MONTH_OFFSET];
            byte day = buffer[DAY_OFFSET];

            return new DateTime((year > DateTime.Now.Year % 1000 ? 1900 : 2000) + year, month, day);
        }

        private LoadColumnsResult LoadColumns(Stream stream, byte firstColumnByte)
        {
            var columns = new List<IColumn>();
            int columnOffset = 0;
            int readBytes = 0;

            // optimization: one byte ahead when loading columns => only one I/O read per column
            var columnBytes = new byte[COLUMN_DESCRIPTOR_SIZE + 1];
            columnBytes[0] = firstColumnByte;

            while (columnBytes[0] != FILE_DESCRIPTOR_TERMINATOR)
            {
                // read first byte of the next column
                readBytes += stream.ReadBlock(columnBytes, 1, COLUMN_DESCRIPTOR_SIZE);

                Column newColumn = ParseColumn(columnBytes, columnOffset);
                columns.Add(newColumn);
                columnOffset += newColumn.Size;

                // we're one byte ahead
                columnBytes[0] = columnBytes.Last();
            }

            return new LoadColumnsResult(columns, readBytes);
        }

        private async Task<LoadColumnsResult> LoadColumnsAsync(Stream stream, byte firstColumnByte, CancellationToken cancellationToken)
        {
            var columns = new List<IColumn>();
            int columnOffset = 0;
            int readBytes = 0;

            // optimization: one byte ahead when loading columns => only one I/O read per column
            var columnBytes = new byte[COLUMN_DESCRIPTOR_SIZE + 1];
            columnBytes[0] = firstColumnByte;

            while (columnBytes[0] != FILE_DESCRIPTOR_TERMINATOR)
            {
                // read first byte of the next column
                readBytes += await stream.ReadBlockAsync(columnBytes, 1, COLUMN_DESCRIPTOR_SIZE, cancellationToken).ConfigureAwait(false);

                Column newColumn = ParseColumn(columnBytes, columnOffset);
                columns.Add(newColumn);
                columnOffset += newColumn.Size;

                // we're one byte ahead
                columnBytes[0] = columnBytes.Last();
            }

            return new LoadColumnsResult(columns, readBytes);
        }

        private Column ParseColumn(byte[] buffer, int columnOffset)
        {
            string name = Encoding.UTF8.GetString(buffer, COLUMN_NAME_OFFSET, COLUMN_NAME_LENGTH).TrimEnd('\0', ' ');
            byte type = buffer[COLUMN_TYPE_OFFSET];
            byte size = buffer[COLUMN_SIZE_OFFSET];

            return CreateColumn(size, type, name, columnOffset);
        }

        /// <summary>
        /// Represents basic properties of dBASE header. Only for internal usage.
        /// </summary>
        public class BasicProperties
        {
            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="lastModified">The date the table was last modified.</param>
            /// <param name="rowCount">The number of rows in a table.</param>
            /// <param name="rowSize">The row size in bytes.</param>
            /// <param name="headerSize">The header total size.</param>
            public BasicProperties(DateTime lastModified, int rowCount, short rowSize, short headerSize)
            {
                LastModified = lastModified;
                RowCount = rowCount;
                RowSize = rowSize;
                HeaderSize = headerSize;
            }

            /// <summary>
            /// Gets the header total size.
            /// </summary>
            public short HeaderSize { get; }

            /// <summary>
            /// Gets the date the table was last modified.
            /// </summary>
            public DateTime LastModified { get; }

            /// <summary>
            /// Gets the number of rows in the table, including deleted ones.
            /// </summary>
            public int RowCount { get; }

            /// <summary>
            /// Gets the row size in bytes.
            /// </summary>
            public short RowSize { get; }
        }

        private sealed class LoadColumnsResult
        {
            public LoadColumnsResult(IList<IColumn> columns, int readBytes)
            {
                Columns = columns;
                ReadBytes = readBytes;
            }

            public IList<IColumn> Columns { get; }

            public int ReadBytes { get; }
        }
    }
}