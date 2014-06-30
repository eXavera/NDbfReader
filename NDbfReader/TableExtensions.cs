using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Extensions for for the <see cref="Table"/> class.
    /// </summary>
    public static class TableExtensions
    {
        /// <summary>
        /// Loads the DBF table into a <see cref="DataTable"/> with the default ASCII encoding.
        /// </summary>
        /// <param name="table">The DBF table to load.</param>
        /// <returns>A <see cref="DataTable"/> loaded from the DBF table.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="table"/> is <c>null</c>.</exception>
        /// <exception cref="InvalidOperationException">Another reader of the DBF table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The DBF table is disposed.</exception>
        public static DataTable AsDataTable(this Table table)
        {
            if(table == null)
            {
                throw new ArgumentNullException("table");
            }

            var dataTable = CreateDataTable(table.Columns);
            FillData(table.Columns, dataTable, table.OpenReader());
            return dataTable;
        }

        /// <summary>
        /// Loads the DBF table into a <see cref="DataTable"/> with the default ASCII encoding.
        /// </summary>
        /// <param name="table">The DBF table to load.</param>
        /// <param name="columnNames">The names of columns to load.</param>
        /// <returns>A <see cref="DataTable"/> loaded from the DBF table.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="table"/> is <c>null</c> or one of the column names is <c>null</c>.</exception>
        /// <exception cref="InvalidOperationException">Another reader of the DBF table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The DBF table is disposed.</exception>
        public static DataTable AsDataTable(this Table table, params string[] columnNames)
        {
            return AsDataTable(table, Encoding.ASCII, columnNames);
        }

         /// <summary>
        /// Loads the DBF table into a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="table">The DBF table to load.</param>
        /// <param name="encoding">The encoding that is used to load the rows content.</param>
        /// <returns>A <see cref="DataTable"/> loaded from the DBF table.</returns>
        /// <exception cref="InvalidOperationException">Another reader of the DBF table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The DBF table is disposed.</exception>
        public static DataTable AsDataTable(this Table table, Encoding encoding)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            if (encoding == null)
            {
                throw new ArgumentNullException("encoding");
            }

            var dataTable = CreateDataTable(table.Columns);
            FillData(table.Columns, dataTable, table.OpenReader(encoding));
            return dataTable;
        }

        /// <summary>
        /// Loads the DBF table into a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="table">The DBF table to load.</param>
        /// <param name="encoding">The encoding that is used to load the rows content.</param>
        /// <param name="columnNames">The names of columns to load.</param>
        /// <returns>A <see cref="DataTable"/> loaded from the DBF table.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="table"/> is <c>null</c> or <paramref name="encoding"/> is <c>null</c> or one of the column names is <c>null</c>.</exception>
        /// <exception cref="InvalidOperationException">Another reader of the DBF table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The DBF table is disposed.</exception>
        public static DataTable AsDataTable(this Table table, Encoding encoding, params string[] columnNames)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            if (encoding == null)
            {
                throw new ArgumentNullException("encoding");
            }

            var selectedColumns = new List<IColumn>(columnNames.Length);
            foreach (var columnName in columnNames)
            {
                var column = table.Columns.FirstOrDefault(c => c.Name == columnName);
                if(column == null)
                {
                    throw new ArgumentOutOfRangeException("columnNames", columnName, "The table does not have a column with this name.");
                }
                selectedColumns.Add(column);
            }

            var dataTable = CreateDataTable(selectedColumns);
            FillData(selectedColumns, dataTable, table.OpenReader(encoding));
            return dataTable;
        }

        private static DataTable CreateDataTable(IEnumerable<IColumn> columns)
        {
            var dataTable = new DataTable()
            {
                Locale = CultureInfo.CurrentCulture
            };

            foreach (var column in columns)
            {
                var columnType = Nullable.GetUnderlyingType(column.Type) ?? column.Type;
                dataTable.Columns.Add(column.Name, columnType);
            }

            return dataTable;
        }

        private static void FillData(IEnumerable<IColumn> columns, DataTable dataTable, Reader reader)
        {
            while (reader.Read())
            {
                var row = dataTable.NewRow();
                foreach (var column in columns)
                {
                    row[column.Name] = reader.GetValue(column) ?? DBNull.Value;
                }
                dataTable.Rows.Add(row);
            }
        }
    }
}
