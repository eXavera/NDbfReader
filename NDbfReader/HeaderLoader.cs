using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a loader of dBASE header used by <see cref="Table"/>.
    /// </summary>
    public class HeaderLoader
    {
        private const byte FILE_DESCRIPTOR_TERMINATOR = 0x0D;

        private static readonly HeaderLoader _default = new HeaderLoader();

        /// <summary>
        /// Gets the default loader.
        /// </summary>
        public static HeaderLoader Default
        {
            get
            {
                return _default;
            }
        }

        /// <summary>
        /// Loads a header from the specified binary reader.
        /// </summary>
        /// <param name="reader">The binary reader positioned on the first byte of a dBASE table.</param>
        /// <returns>A header loaded from the specified reader.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="reader"/> is <c>null</c>.</exception>
        public virtual Header Load(BinaryReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }

            /*
           * 0    = signature
           */
            int position = 0;
            SkipHeaderBytes(reader, position, 1);
            position += 1;

            DateTime lastModified = LoadLastModifiedDate(reader);

            int rowCount = reader.ReadInt32();
            position += 4;

            /*
            * 8-9  = header size
            */
            SkipHeaderBytes(reader, position, 2);
            position += 2;

            short rowSize = reader.ReadInt16();
            position += 2;

            /*
             * 12-13  = reserved
             * 14     = Flag indicating incomplete dBASE IV transaction.
             * 15     = dBASE IV encryption flag
             * 16-27  = Reserved for multi-user processing.
             * 28     = Production MDX flag; 0x01 if a production .MDX file exists for this table; 0x00 if no .MDX file exists.
             * 29     = Language driver ID.
             * 30-31  = Reserved; filled with zeros
             */
            SkipHeaderBytes(reader, position, 20);

            return LoadColumns(reader, lastModified, rowCount, rowSize);
        }

        /// <summary>
        /// Creates a header instance.
        /// </summary>
        /// <param name="lastModified">The date the table was last modified.</param>
        /// <param name="rowCount">The number of rows.</param>
        /// <param name="rowSize">The size of a row in bytes.</param>
        /// <param name="columns">The loaded columns.</param>
        /// <returns>A header instance.</returns>
        protected virtual Header CreateHeader(DateTime lastModified, int rowCount, short rowSize, IList<IColumn> columns)
        {
            return new Header(lastModified, rowCount, rowSize, columns);
        }

        /// <summary>
        /// Loads a column from the specified properties.
        /// </summary>
        /// <param name="reader">The reader instance.</param>
        /// <param name="type">A byte that represents the column type.</param>
        /// <param name="name">The column name.</param>
        /// <param name="columnOffset">The column offset (in bytes) in a row.</param>
        /// <returns>A column instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="name"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="columnOffset"/> is &lt; 0.</exception>
        /// <exception cref="NotSupportedException">value of <paramref name="type"/> parameter is not supported.</exception>
        protected virtual Column LoadColumn(BinaryReader reader, byte type, string name, int columnOffset)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }
            if (columnOffset < 0)
            {
                throw new ArgumentOutOfRangeException("columnOffset");
            }

            //11-14 reserved
            reader.BaseStream.SeekForward(4);

            byte size = reader.ReadByte();

            /*
             * 16       = decimal count
             * 17-18    = reserved
             * 19       = work area ID
             * 20-21    = reserved
             * 22       = set fields flag
             * 23-29    = reserved
             * 30       = index file flag
             */
            reader.BaseStream.SeekForward(15);

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
                    throw new NotSupportedException($"The {name} column's type is not supported.");
            }
        }

        /// <summary>
        /// Skips the specified number of header bytes.
        /// </summary>
        /// <param name="reader">The reader instance.</param>
        /// <param name="offset">The current offset (in bytes) from the begining of the table header.</param>
        /// <param name="count">The number of bytes to skip.</param>
        protected virtual void SkipHeaderBytes(BinaryReader reader, int offset, int count)
        {
            reader.BaseStream.SeekForward(count);
        }

        private static DateTime LoadLastModifiedDate(BinaryReader reader)
        {
            // expected format YYMMDD
            byte[] bytes = reader.ReadBytes(3);
            byte year = bytes[0];
            byte month = bytes[1];
            byte day = bytes[2];
            return new DateTime((year > DateTime.Now.Year % 1000 ? 1900 : 2000) + year, month, day);
        }

        private static string ReadColumnName(BinaryReader reader)
        {
            byte firstByte = reader.ReadByte();
            if (firstByte == FILE_DESCRIPTOR_TERMINATOR)
            {
                return null;
            }

            var nameBytesBuffer = new byte[11];
            nameBytesBuffer[0] = firstByte;
            reader.Read(nameBytesBuffer, 1, 10);
            return Encoding.ASCII.GetString(nameBytesBuffer).TrimEnd('\0', ' ');
        }

        private Header LoadColumns(BinaryReader reader, DateTime lastModified, int rowCount, short rowSize)
        {
            var columns = new List<IColumn>();
            Column newColumn = null;
            int columnOffset = 0;

            while ((newColumn = LoadNextColumn(reader, columnOffset)) != null)
            {
                columns.Add(newColumn);
                columnOffset += newColumn.Size;
            }

            return CreateHeader(lastModified, rowCount, rowSize, columns);
        }

        private Column LoadNextColumn(BinaryReader reader, int columnOffset)
        {
            string name = ReadColumnName(reader);
            if (name == null)
            {
                return null;
            }

            byte typeByte = reader.ReadByte();

            return LoadColumn(reader, typeByte, name, columnOffset);
        }
    }
}