﻿using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a date column.
    /// </summary>
    [DebuggerDisplay("Date {Name}")]
    public class DateTimeColumn : Column<DateTime?>
    {
        private const int NUMBER_OF_DATE_CHARS = 8;

#if NETSTANDARD_21
        private char[] _valueCharsBuffer;
#endif

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
        [Obsolete("Specify the actual column size")]
        public DateTimeColumn(string name, int offset)
            : base(name, offset, NUMBER_OF_DATE_CHARS, 0)
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
        public DateTimeColumn(string name, int offset, int size)
            : base(name, offset, size, 0)
        {
            if (size < NUMBER_OF_DATE_CHARS)
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
            const string DateFormat = "yyyyMMdd";
            IFormatProvider NoFormatProvider = null;

#if NETSTANDARD_21 // avoids string allocation
            if (_valueCharsBuffer == null)
            {
                _valueCharsBuffer = new char[NUMBER_OF_DATE_CHARS];
            }

            int read = encoding.GetChars(buffer, offset, NUMBER_OF_DATE_CHARS, _valueCharsBuffer, charIndex: 0);
            if (read != NUMBER_OF_DATE_CHARS || char.IsWhiteSpace(_valueCharsBuffer[0]))
            {
                return null;
            }

            return DateTime.ParseExact(_valueCharsBuffer, DateFormat, NoFormatProvider);
#else
            string stringValue = encoding.GetString(buffer, offset, NUMBER_OF_DATE_CHARS);
            if (string.IsNullOrWhiteSpace(stringValue))
            {
                return null;
            }

            return DateTime.ParseExact(stringValue, DateFormat, NoFormatProvider);
#endif
        }
    }
}