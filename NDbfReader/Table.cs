using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NDbfReader
{
    /// <summary>
    /// Represents a dBASE table. Use one of the Open static methods to create a new instance.
    /// </summary>
    /// <example>
    /// <code>
    /// using(var table = Table.Open(@"D:\Example\table.dbf"))
    /// {
    ///     ...
    /// }
    /// </code>
    /// </example>
    public class Table : IParentTable, IDisposable
    {
        internal static readonly Encoding DefaultEncoding = Encoding.UTF8;

        private readonly Header _header;
        private readonly Stream _stream;
        private bool _disposed;
        private bool _isReaderOpened;

        /// <summary>
        /// Initializes a new instance from the specified header and input stream.
        /// </summary>
        /// <param name="header">The dBASE header.</param>
        /// <param name="stream">The input stream positioned at the first byte of the first row.</param>
        /// <exception cref="ArgumentNullException"><paramref name="header"/> is <c>null</c> or <paramref name="stream"/> is <c>null</c>.</exception>
        protected Table(Header header, Stream stream)
        {
            if (header == null)
            {
                throw new ArgumentNullException(nameof(header));
            }
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            _stream = stream;
            _header = header;
        }

        /// <summary>
        /// Gets the columns of the table.
        /// </summary>
        /// <exception cref="ObjectDisposedException">The table is disposed.</exception>
        public ColumnCollection Columns
        {
            get
            {
                ThrowIfDisposed();

                return _header.Columns;
            }
        }

        Header IParentTable.Header
        {
            get { return Header; }
        }

        Stream IParentTable.Stream
        {
            get { return _stream; }
        }

        /// <summary>
        /// Gets the date the table was last modified.
        /// </summary>
        public DateTime LastModified
        {
            get
            {
                ThrowIfDisposed();

                return _header.LastModified;
            }
        }

        /// <summary>
        /// Gets the dBASE header.
        /// </summary>
        protected Header Header
        {
            get
            {
                return _header;
            }
        }

        /// <summary>
        /// Opens a table from the specified file.
        /// </summary>
        /// <param name="path">The file to be opened.</param>
        /// <returns>A table instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <c>null</c> or empty.</exception>
        public static Table Open(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            return Open(File.OpenRead(path));
        }

        /// <summary>
        /// Opens a table from the specified stream.
        /// </summary>
        /// <param name="stream">The stream of dBASE table to open. The stream is closed when the returned table instance is disposed.</param>
        /// <returns>A table instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="stream"/> does not allow reading.</exception>
        public static Table Open(Stream stream)
        {
            return Open(stream, HeaderLoader.Default);
        }

        /// <summary>
        /// Opens a table from the specified stream with the specified header loader.
        /// </summary>
        /// <param name="stream">The stream of dBASE table to open. The stream is closed when the returned table instance is disposed.</param>
        /// <param name="headerLoader">The header loader.</param>
        /// <returns>A table instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c> or <paramref name="headerLoader"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="stream"/> does not allow reading.</exception>
        public static Table Open(Stream stream, HeaderLoader headerLoader)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            if (!stream.CanRead)
            {
                throw new ArgumentException($"The stream does not allow reading ({nameof(stream.CanRead)} property returns false).", nameof(stream));
            }
            if (headerLoader == null)
            {
                throw new ArgumentNullException(nameof(headerLoader));
            }

            Header header = headerLoader.Load(stream);
            return new Table(header, stream);
        }

        /// <summary>
        /// Opens a table from the specified file.
        /// </summary>
        /// <param name="path">The file to be opened.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A table instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <c>null</c> or empty.</exception>
        public static Task<Table> OpenAsync(string path, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            const int DEFAULT_BUFFER = 4096;
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, DEFAULT_BUFFER, useAsync: true);

            return OpenAsync(stream, cancellationToken);
        }

        /// <summary>
        /// Opens a table from the specified stream.
        /// </summary>
        /// <param name="stream">The stream of dBASE table to open. The stream is closed when the returned table instance is disposed.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A table instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="stream"/> does not allow reading.</exception>
        public static Task<Table> OpenAsync(Stream stream, CancellationToken cancellationToken = default(CancellationToken))
        {
            return OpenAsync(stream, HeaderLoader.Default, cancellationToken);
        }

        /// <summary>
        /// Opens a table from the specified stream with the specified header loader.
        /// </summary>
        /// <param name="stream">The stream of dBASE table to open. The stream is closed when the returned table instance is disposed.</param>
        /// <param name="headerLoader">The header loader.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A table instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c> or <paramref name="headerLoader"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="stream"/> does not allow reading.</exception>
        /// <exception cref="NotSupportedException">The dBASE table contains one or more columns of unsupported type.</exception>
        public static async Task<Table> OpenAsync(Stream stream, HeaderLoader headerLoader, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            if (!stream.CanRead)
            {
                throw new ArgumentException($"The stream does not allow reading ({nameof(stream.CanRead)} property returns false).", nameof(stream));
            }
            if (headerLoader == null)
            {
                throw new ArgumentNullException(nameof(headerLoader));
            }

            Header header = await headerLoader.LoadAsync(stream, cancellationToken).ConfigureAwait(false);
            return new Table(header, stream);
        }

        /// <summary>
        /// Closes the underlying stream.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Disposing();
            _disposed = true;
        }

        void IParentTable.ThrowIfDisposed()
        {
            ThrowIfDisposed();
        }

        /// <summary>
        /// Opens a reader of the table with the default <c>UTF-8</c> encoding. Only one reader per table can be opened.
        /// </summary>
        /// <returns>A <see cref="Reader"/> instance.</returns>
        /// <exception cref="InvalidOperationException">Another reader of the table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The table is disposed.</exception>
        public Reader OpenReader()
        {
            return OpenReader(DefaultEncoding);
        }

        /// <summary>
        /// Opens a reader of the table with the specified encoding. Only one reader per table can be opened.
        /// </summary>
        /// <param name="encoding">The encoding that is used to load the table content.</param>
        /// <returns>A <see cref="Reader"/> instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="encoding"/> is <c>null</c>.</exception>
        /// <exception cref="InvalidOperationException">Another reader of the table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The table is disposed.</exception>
        public virtual Reader OpenReader(Encoding encoding)
        {
            if (encoding == null)
            {
                throw new ArgumentNullException(nameof(encoding));
            }

            ThrowIfDisposed();

            if (_isReaderOpened)
            {
                throw new InvalidOperationException("The table can open only one reader.");
            }
            _isReaderOpened = true;

            return CreateReader(encoding);
        }

        /// <summary>
        /// Creates a <see cref="Reader"/> instance.
        /// </summary>
        /// <param name="encoding">The encoding that is passed to the new <see cref="Reader"/> instance.</param>
        /// <returns>A <see cref="Reader"/> instance.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="encoding"/> is <c>null</c>.</exception>
        protected virtual Reader CreateReader(Encoding encoding)
        {
            return new Reader(this, encoding);
        }

        /// <summary>
        /// Releases the underlying stream.
        /// <remarks>
        /// The method is called only when the <see cref="Dispose"/> method is called for the first time.
        /// You MUST always call the base implementation.
        /// </remarks>
        /// </summary>
        protected virtual void Disposing()
        {
            _stream.Dispose();
        }

        /// <summary>
        /// Throws a <see cref="ObjectDisposedException"/> if the table is already disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }
    }
}