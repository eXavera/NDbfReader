using System;
using System.Collections;
using System.Collections.Generic;

namespace NDbfReader
{
    /// <summary>
    /// Represents a collection of columns.
    /// </summary>
    public sealed class ColumnCollection
#if NET40
        : IEnumerable<IColumn>
#else
        : IReadOnlyList<IColumn>
#endif
    {
        private readonly IList<IColumn> _list;
        private Dictionary<string, IColumn> _dictionary;
        private HashSet<IColumn> _hashSet;

        /// <summary>
        /// Initializes a new instance with the specified columns.
        /// </summary>
        /// <param name="columns">The source columns.</param>
        public ColumnCollection(IList<IColumn> columns)
        {
            if (columns == null)
            {
                throw new ArgumentNullException(nameof(columns));
            }

            _list = columns;
        }

        /// <summary>
        /// Gets the number of columns.
        /// </summary>
        public int Count => _list.Count;

        private Dictionary<string, IColumn> Dictionary
        {
            get
            {
                if (_dictionary == null)
                {
                    _dictionary = new Dictionary<string, IColumn>(_list.Count, StringComparer.Ordinal);
                    foreach (IColumn column in _list)
                    {
                        _dictionary.Add(column.Name, column);
                    }
                }
                return _dictionary;
            }
        }

        private HashSet<IColumn> HashSet
        {
            get
            {
                if (_hashSet == null)
                {
                    _hashSet = new HashSet<IColumn>(_list);
                }
                return _hashSet;
            }
        }

        /// <summary>
        /// Gets the column at the specified index.
        /// </summary>
        /// <param name="index">The index of the column.</param>
        /// <returns>The column at the specified index.</returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="index"/> is &lt; 0.<br />
        /// - or -<br />
        /// <paramref name="index"/> is equal to or greater than <see cref="Count"/>.
        /// </exception>
        public IColumn this[int index] => _list[index];

        /// <summary>
        /// Gets the column with the specified name.
        /// </summary>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The column with the specified name.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">No column with the <paramref name="columnName"/> was found.</exception>
        public IColumn this[string columnName]
        {
            get
            {
                if (columnName == null)
                {
                    throw new ArgumentNullException(nameof(columnName));
                }

                IColumn column;
                if (Dictionary.TryGetValue(columnName, out column))
                {
                    return column;
                }
                throw new ArgumentOutOfRangeException(nameof(columnName));
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<IColumn> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        internal bool Contains(IColumn column)
        {
            return HashSet.Contains(column);
        }

        internal IColumn FindByName(string columnName)
        {
            IColumn column;
            if (Dictionary.TryGetValue(columnName, out column))
            {
                return column;
            }
            return null;
        }
    }
}