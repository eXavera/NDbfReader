using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a <see cref="decimal"/> column.
    /// </summary>
    [DebuggerDisplay("Decimal {Name}")]
    public class DecimalColumn : Column<decimal?>
    {
        private static readonly NumberFormatInfo DecimalNumberFormat = new NumberFormatInfo { NumberDecimalSeparator = "." };

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <param name="decimalPrecision">The column decimal precision.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
        public DecimalColumn(string name, int offset, int size, int decimalPrecision)
            : base(name, offset, size, decimalPrecision)
        {
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="offset">The offset in <paramref name="buffer"/> at which to begin loading the value.</param>
        /// <param name="encoding">The encoding to use to parse the value.</param>
        /// <returns>A column value.</returns>
        protected override decimal? DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            string stringValue = encoding.GetString(buffer, offset, Size);
            if (stringValue.Length == 0)
            {
                return null;
            }

            decimal value;
            if (decimal.TryParse(stringValue, NumberStyles.Float | NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite, DecimalNumberFormat, out value))
            {
                return value;
            }
            return null;
        }
    }
}