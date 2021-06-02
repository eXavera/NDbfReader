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
        private static readonly NumberFormatInfo NumberFormat = new NumberFormatInfo { NumberDecimalSeparator = "." };

#if NETSTANDARD_21
        private char[] _valueCharsBuffer;
#endif

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <param name="decimalPrecision">The column decimal precision.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0 or <paramref name="decimalPrecision"/> is &lt; 0.</exception>
        public DecimalColumn(string name, int offset, int size, int decimalPrecision)
            : base(name, offset, size, decimalPrecision)
        {
        }

        /// <summary>
        /// This overload is <b>obsolete</b>. Use <c>DecimalColumn(string name, int offset, int size, int decimalPrecision)</c> instead.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="NotSupportedException">This constructor is obsolete.</exception>
        [Obsolete("This overload is no loner used. Use DecimalColumn(string name, int offset, int size, int decimalPrecision) instead", error: true)]
        public DecimalColumn(string name, int offset, int size)
            : base(name, offset, size, 0)
        {
            throw new NotSupportedException(
                "This coverload is no loner used. Use DecimalColumn(string name, int offset, int size, int decimalPrecision) instead");
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
            const NumberStyles NumberStyle = NumberStyles.Float | NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite;

#if NETSTANDARD_21 // avoids string allocation
            if (_valueCharsBuffer == null)
            {
                _valueCharsBuffer = new char[Size];
            }

            int read = encoding.GetChars(buffer, offset, Size, _valueCharsBuffer, 0);
            if (read > 0 && decimal.TryParse(_valueCharsBuffer, NumberStyle, NumberFormat, out decimal value))
            {
                return value;
            }
            return null;
#else
            string stringValue = encoding.GetString(buffer, offset, Size);
            if (stringValue.Length > 0 && decimal.TryParse(stringValue, NumberStyle, NumberFormat, out decimal value))
            {
                return value;
            }
            return null;
#endif
        }
    }
}