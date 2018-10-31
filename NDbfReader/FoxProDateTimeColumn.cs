using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a FoxPro date time column.
    /// </summary>
    [DebuggerDisplay("DateTime {Name}")]
    public class FoxProDateTimeColumn : Column<DateTime?>
    {
        private const int MIN_SIZE = 8;

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
        [Obsolete("Specify the actual column size")]
        public FoxProDateTimeColumn(string name, int offset)
            : base(name, offset, MIN_SIZE)
        {
        }

        /// <summary>
        /// Initializes a new instance with the specified name, offset and size.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 8.</exception>
        public FoxProDateTimeColumn(string name, int offset, int size)
            : base(name, offset, size)
        {
            if (size < MIN_SIZE)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="offset">The offset in <paramref name="buffer"/> at which to begin loading the value.</param>
        /// <param name="encoding">The encoding to use to parse the value.</param>
        /// <returns>A column value.</returns>
        protected override DateTime? DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            uint days = BitConverter.ToUInt32(buffer, offset);
            if (days == 0)
            {
                return null;
            }

            const uint ZERO_DATE = 1721426;
            uint daysFromZeroDate = days - ZERO_DATE;
            uint miliseconds = BitConverter.ToUInt32(buffer, offset + 4);

            return DateTime.MinValue.AddDays(daysFromZeroDate).AddMilliseconds(miliseconds);
        }
    }
}