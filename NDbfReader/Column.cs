using System;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// The base class of all column types. Intended for internal usage. To define a custom column type, derive from the generic subclass <see cref="DbfColumn&lt;T&gt;"/>.
    /// </summary>
    public abstract class Column : IColumn
    {
        private readonly string _name;
        private readonly int _size;
        private readonly int _offset;

        /// <summary>
        /// Initializes a new instance with the specified name, offset and size.
        /// </summary>
        /// <param name="name">The colum name.</param>
        /// <param name="offset">The column offset in a row in bytes.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 1.</exception>
        protected internal Column(string name, int offset, int size)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException("offset");
            }
            if (size < 1)
            {
                throw new ArgumentOutOfRangeException("size");
            }

            _name = name;
            _offset = offset;
            _size = size;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string Name
        {
            get
            {
                return _name;
            }
        }

        /// <summary>
        /// Gets the column offset in a row in bytes.
        /// </summary>
        public int Offset
        {
            get
            {
                return _offset;
            }
        }

        /// <summary>
        /// Gets the column size in bytes.
        /// </summary>
        public int Size
        {
            get
            {
                return _size;
            }
        }

        /// <summary>
        /// Gets the <c>CLR</c> type of a column value.
        /// </summary>
        public abstract Type Type { get; }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="encoding">The encoding that should be used when loading a value.</param>
        /// <returns>A column value.</returns>
        public abstract object LoadValueAsObject(byte[] buffer, Encoding encoding);
    }

    /// <summary>
    /// The base class for all column types.
    /// </summary>
    /// <typeparam name="T">The type of the column value.</typeparam>
    public abstract class DbfColumn<T> : Column
    {
        /// <summary>
        /// Initializes a new instance with the specified name, offset and size.
        /// </summary>
        /// <param name="name">The colum name.</param>
        /// <param name="offset">The column offset in a row in bytes.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 1.</exception>
        protected DbfColumn(string name, int offset, int size)
            : base(name, offset, size)
        {
        }

        /// <summary>
        /// Gets the <c>CLR</c> type of column value.
        /// </summary>
        public override Type Type
        {
            get
            {
                return typeof(T);
            }
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="encoding">The encoding that should be used when loading a value.</param>
        /// <returns>A column value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <c>null</c> or <paramref name="encoding"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="buffer"/> is smaller then the size of the column.</exception>
        public T LoadValue(byte[] buffer, Encoding encoding)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException("buffer");
            }
            if (buffer.Length < Size)
            {
                throw new ArgumentException(string.Format("The buffer must have at least {0} bytes.", Size));
            }
            if (encoding == null)
            {
                throw new ArgumentNullException("encoding");
            }

            return DoLoad(buffer, encoding);
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded.</param>
        /// <param name="encoding">The encoding that should be used when loading a value.</param>
        /// <returns>A column value.</returns>
        public sealed override object LoadValueAsObject(byte[] buffer, Encoding encoding)
        {
            return LoadValue(buffer, encoding);
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
        /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
        /// <returns>A column value.</returns>
        protected abstract T DoLoad(byte[] buffer, Encoding encoding);
    }
}
