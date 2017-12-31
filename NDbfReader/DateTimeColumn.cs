using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a <see cref="DateTime"/> column.
    /// </summary>
    [DebuggerDisplay("DateTime {Name}")]
    public class DateTimeColumn : Column<DateTime?>
    {
        private const int SIZE = 8;

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row in bytes.</param>
        /// <param name="nativeFormat">The dBASE date format of the column.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
        public DateTimeColumn(string name, int offset, DateTimeNativeFormat nativeFormat = DateTimeNativeFormat.Default)
            : base(name, offset, SIZE)
        {
            NativeFormat = nativeFormat;
        }

        /// <summary>
        /// Gets the dBASE date format of the column.
        /// </summary>
        protected DateTimeNativeFormat NativeFormat { get; }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="offset">The byte offset in <paramref name="buffer"/> at which loading begins. </param>
        /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
        /// <returns>A column value.</returns>
        protected override DateTime? DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            if (NativeFormat == DateTimeNativeFormat.FoxPro)
            {
                return ParseFoxProDateTime(buffer, offset);
            }
            return ParseDate(buffer, offset, encoding);
        }

        private static DateTime? ParseDate(byte[] buffer, int offset, Encoding encoding)
        {
            string stringValue = encoding.GetString(buffer, offset, SIZE);
            if (string.IsNullOrWhiteSpace(stringValue))
            {
                return null;
            }
            return DateTime.ParseExact(stringValue, "yyyyMMdd", null, DateTimeStyles.AllowLeadingWhite | DateTimeStyles.AllowTrailingWhite);
        }

        private static DateTime? ParseFoxProDateTime(byte[] buffer, int offset)
        {
            // https://www.experts-exchange.com/questions/20585197/Where-is-the-internal-storage-format-of-the-FoxPro-DateTime-data-type-documented.html#a10149637
            const uint ZERO_DATE = 1721426;

            uint daysFromZeroDate = BitConverter.ToUInt32(buffer, offset) - ZERO_DATE;
            uint miliseconds = BitConverter.ToUInt32(buffer, offset + 4);

            return new DateTime(1, 1, 1).AddDays(daysFromZeroDate).AddMilliseconds(miliseconds);
        }
    }
}