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
        private Encoding _lastEncoding;
        private byte[] _spacePattern;
        private byte[] _zeroPattern;

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row in bytes.</param>
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
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="offset">The byte offset in <paramref name="buffer"/> at which loading begins. </param>
        /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
        /// <returns>A column value.</returns>
        protected override string DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            // Why don't just call string.TrimEnd method? Because the method allocates a lot of objects.
            if (_lastEncoding == null || _lastEncoding != encoding)
            {
                _lastEncoding = encoding;
                _zeroPattern = encoding.GetBytes(new[] { '\0' });
                _spacePattern = encoding.GetBytes(new[] { ' ' });
            }

            int size = GetSizeOfTrimString(buffer, offset, offset + Size, _spacePattern, _zeroPattern);
            if (size == 0)
            {
                return null;
            }
            return encoding.GetString(buffer, offset, size);
        }

        private static bool EndsWith(byte[] input, int endIndex, byte[] values)
        {
            if (endIndex < values.Length) return false;

            int inputStartIndex = endIndex - values.Length;
            for (int i = 0; i < values.Length; i++)
            {
                if (input[inputStartIndex + i] != values[i])
                {
                    return false;
                }
            }
            return true;
        }

        private static int GetSizeOfTrimString(byte[] buffer, int startIndex, int endIndex, byte[] spacePattern, byte[] zeroPattern)
        {
            // both patterns have the same length
            int patternLength = spacePattern.Length;
            for (int i = endIndex; i >= startIndex; i -= patternLength)
            {
                if (!EndsWith(buffer, i, spacePattern) && !EndsWith(buffer, i, zeroPattern))
                {
                    return i - startIndex;
                }
            }
            return 0;
        }
    }
}