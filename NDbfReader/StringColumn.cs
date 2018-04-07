using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a <see cref="string"/> column.
    /// </summary>
    [DebuggerDisplay("String {Name}")]
    public class StringColumn : Column<string>
    {
        private char[] _parsedCharsBuffer;

        /// <summary>
        /// Initializes a new instance with the specified name, offset and size.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
        public StringColumn(string name, int offset, int size)
            : base(name, offset, size)
        {
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="offset">The offset in <paramref name="buffer"/> at which to begin loading the value.</param>
        /// <param name="encoding">The encoding to use to parse the value.</param>
        /// <returns>A column value.</returns>
        protected override string DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            if (_parsedCharsBuffer == null)
            {
                _parsedCharsBuffer = new char[Size];
            }

            int readCharsCount = encoding.GetChars(buffer, offset, Size, _parsedCharsBuffer, 0);
            for (int trimSize = readCharsCount; trimSize > 0; trimSize--)
            {
                char c = _parsedCharsBuffer[trimSize - 1];
                if (c != '\0' && c != ' ')
                {
                    return new String(_parsedCharsBuffer, 0, trimSize);
                }
            }

            return null;
        }
    }
}