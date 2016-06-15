using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        /// Loads a header from the specified binary reader.
        /// </summary>
        /// <param name="stream">The input stream on the first byte of a dBASE table.</param>
        /// <returns>A header loaded from the specified reader.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c>.</exception>
        public virtual Header Load(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // optimization: be one byte ahead when loading columns => so we have only one I/O read per column
            var buffer = new byte[HEADER_SIZE + 1];
            stream.Read(buffer, 0, buffer.Length);

            BasicProperties properties = ParseBasicProperties(buffer);

            return LoadColumns(stream, properties, buffer.Last());
        }

        /// <summary>
        /// Loads a header from the specified binary reader.
        /// </summary>
        /// <param name="stream">The input stream on the first byte of a dBASE table.</param>
        /// <returns>A header loaded from the specified reader.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c>.</exception>
        public virtual async Task<Header> LoadAsync(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // optimization: be one byte ahead when loading columns => so we have only one I/O read per column
            var buffer = new byte[HEADER_SIZE + 1];
            await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);

            BasicProperties properties = ParseBasicProperties(buffer);

            return await LoadColumnsAsync(stream, properties, buffer.Last()).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a column based on the specified properties.
        /// </summary>
        /// <param name="size">The column size in bytes.</param>
        /// <param name="type">The column type.</param>
        /// <param name="name">The column name.</param>
        /// <param name="columnOffset">The column offset (in bytes) in a row.</param>
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
                    return new DateTimeColumn(name, columnOffset);

                case NativeColumnType.Long:
                    return new Int32Column(name, columnOffset);

                case NativeColumnType.Logical:
                    return new BooleanColumn(name, columnOffset);

                case NativeColumnType.Numeric:
                case NativeColumnType.Float:
                    return new DecimalColumn(name, columnOffset, size);

                default:
                    return new UnsupportedColumn(name, columnOffset, size, type);
            }
        }

        /// <summary>
        /// Creates a header instance.
        /// </summary>
        /// <param name="properties">The header properties.</param>
        /// <param name="columns">The loaded columns.</param>
        /// <returns>A header instance.</returns>
        protected virtual Header CreateHeader(BasicProperties properties, IList<IColumn> columns)
        {
            return new Header(properties.LastModified, properties.RowCount, properties.RowSize, columns);
        }

        /// <summary>
        /// Parsers basic header properties from the specified buffer.
        /// </summary>
        /// <param name="buffer">The header buffer.</param>
        /// <returns>A <see cref="BasicProperties"/> instance.</returns>
        protected virtual BasicProperties ParseBasicProperties(byte[] buffer)
        {
            DateTime lastModified = ParseLastModifiedDate(buffer);
            int rowCount = BitConverter.ToInt32(buffer, ROW_COUNT_OFFSET);
            short rowSize = BitConverter.ToInt16(buffer, ROW_SIZE_OFFSET);

            return new BasicProperties(lastModified, rowCount, rowSize);
        }

        private static DateTime ParseLastModifiedDate(byte[] buffer)
        {
            byte year = buffer[YEAR_OFFSET];
            byte month = buffer[MONTH_OFFSET];
            byte day = buffer[DAY_OFFSET];

            return new DateTime((year > DateTime.Now.Year % 1000 ? 1900 : 2000) + year, month, day);
        }

        private Header LoadColumns(Stream stream, BasicProperties properties, byte firstColumnByte)
        {
            var columns = new List<IColumn>();
            int columnOffset = 0;

            // optimization: be one byte ahead when loading columns => so we have only one I/O read per column
            var columnBytes = new byte[COLUMN_DESCRIPTOR_SIZE + 1];
            columnBytes[0] = firstColumnByte;

            while (columnBytes[0] != FILE_DESCRIPTOR_TERMINATOR)
            {
                // read first byte of next column
                stream.Read(columnBytes, 1, COLUMN_DESCRIPTOR_SIZE);

                Column newColumn = ParseColumn(columnBytes, columnOffset);
                columns.Add(newColumn);
                columnOffset += newColumn.Size;

                // move the first byte of next column where it should be
                columnBytes[0] = columnBytes.Last();
            }

            return CreateHeader(properties, columns);
        }

        private async Task<Header> LoadColumnsAsync(Stream stream, BasicProperties properties, byte firstColumnByte)
        {
            var columns = new List<IColumn>();
            int columnOffset = 0;

            // optimization: be one byte ahead when loading columns => so we have only one I/O read per column
            var columnBytes = new byte[COLUMN_DESCRIPTOR_SIZE + 1];
            columnBytes[0] = firstColumnByte;

            while (columnBytes[0] != FILE_DESCRIPTOR_TERMINATOR)
            {
                // read first byte of next column
                await stream.ReadAsync(columnBytes, 1, COLUMN_DESCRIPTOR_SIZE).ConfigureAwait(false);

                Column newColumn = ParseColumn(columnBytes, columnOffset);
                columns.Add(newColumn);
                columnOffset += newColumn.Size;

                // move the first byte of next column where it should be
                columnBytes[0] = columnBytes.Last();
            }

            return CreateHeader(properties, columns);
        }

        private Column ParseColumn(byte[] buffer, int columnOffset)
        {
            string name = Encoding.ASCII.GetString(buffer, COLUMN_NAME_OFFSET, COLUMN_NAME_LENGTH).TrimEnd('\0', ' ');
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
            /// <param name="lastModified">The date a table was last modified.</param>
            /// <param name="rowCount">The number of rows in a table.</param>
            /// <param name="rowSize">The size of a row in bytes.</param>
            public BasicProperties(DateTime lastModified, int rowCount, short rowSize)
            {
                LastModified = lastModified;
                RowCount = rowCount;
                RowSize = rowSize;
            }

            /// <summary>
            /// Gets a date the table was last modified.
            /// </summary>
            public DateTime LastModified { get; }

            /// <summary>
            /// Gets the number of rows in the table, including deleted ones.
            /// </summary>
            public int RowCount { get; }

            /// <summary>
            /// Gets the size of a row in bytes.
            /// </summary>
            public short RowSize { get; }
        }
    }
}