using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a <see cref="String"/> column.
    /// </summary>
    [DebuggerDisplay("Int32 {Name}")]
    public class StringColumn : Column<string>
    {
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
        /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
        /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
        /// <returns>A column value.</returns>
        protected override string DoLoad(byte[] buffer, Encoding encoding)
        {
            var value = encoding.GetString(buffer, 0, buffer.Length).TrimEnd('\0', ' ');

            return string.IsNullOrWhiteSpace(value) ? null : value;
        }
    }
}
