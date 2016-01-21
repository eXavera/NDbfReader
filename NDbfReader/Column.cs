using System;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// The base class of all column types. Intended for internal usage. To define a custom column type, derive from the generic subclass <see cref="Column&lt;T&gt;"/>.
    /// </summary>
    public abstract class Column : IColumn
    {
        /// <summary>
        /// Initializes a new instance with the specified name, offset and size.
        /// </summary>
        /// <param name="name">The colum name.</param>
        /// <param name="offset">The column offset in a row in bytes.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
        protected internal Column(string name, int offset, int size)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }

            Name = name;
            Offset = offset;
            Size = size;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the column offset in a row in bytes.
        /// </summary>
        public int Offset { get; }

        /// <summary>
        /// Gets the column size in bytes.
        /// </summary>
        public int Size { get; }

        /// <summary>
        /// Gets the <c>CLR</c> type of a column value.
        /// </summary>
        public abstract Type Type { get; }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="offset">The byte offset in <paramref name="buffer"/> at which loading begins. </param>
        /// <param name="encoding">The encoding that should be used when loading a value.</param>
        /// <returns>A column value.</returns>
        public abstract object LoadValueAsObject(byte[] buffer, int offset, Encoding encoding);
    }

    /// <summary>
    /// The base class for all column types.
    /// </summary>
    /// <typeparam name="T">The type of the column value.</typeparam>
    public abstract class Column<T> : Column
    {
        /// <summary>
        /// Initializes a new instance with the specified name, offset and size.
        /// </summary>
        /// <param name="name">The colum name.</param>
        /// <param name="offset">The column offset in a row in bytes.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
        protected Column(string name, int offset, int size)
            : base(name, offset, size)
        {
        }

        /// <summary>
        /// Gets the <c>CLR</c> type of column value.
        /// </summary>
        public override Type Type => typeof(T);

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="offset">The byte offset in <paramref name="buffer"/> at which loading begins. </param>
        /// <param name="encoding">The encoding that should be used when loading a value.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <c>null</c> or <paramref name="encoding"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> &lt; 0.</exception>
        public T LoadValue(byte[] buffer, int offset, Encoding encoding)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (encoding == null)
            {
                throw new ArgumentNullException(nameof(encoding));
            }

            return DoLoad(buffer, offset, encoding);
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="offset">The byte offset in <paramref name="buffer"/> at which loading begins. </param>
        /// <param name="encoding">The encoding that should be used when loading a value.</param>
        /// <returns>A column value.</returns>
        public sealed override object LoadValueAsObject(byte[] buffer, int offset, Encoding encoding)
        {
            return LoadValue(buffer, offset, encoding);
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="offset">The byte offset in <paramref name="buffer"/> at which loading begins. </param>
        /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
        /// <returns>A column value.</returns>
        protected abstract T DoLoad(byte[] buffer, int offset, Encoding encoding);
    }
}