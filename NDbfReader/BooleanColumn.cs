using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a <see cref="bool"/> column.
    /// </summary>
    [DebuggerDisplay("Boolean {Name}")]
    public class BooleanColumn : Column<bool?>
    {
        private const int MIN_SIZE = 1;

        /// <summary>
        /// Initializes a new instance with the specified name and offset.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
        [Obsolete("Specify the actual column size")]
        public BooleanColumn(string name, int offset)
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
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 1.</exception>
        public BooleanColumn(string name, int offset, int size)
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
        protected override bool? DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            char charValue = char.ToUpper((char)buffer[offset]);
            switch (charValue)
            {
                case 'T':
                case 'Y':
                    return true;

                case 'F':
                case 'N':
                    return false;

                default:
                    return null;
            }
        }
    }
}