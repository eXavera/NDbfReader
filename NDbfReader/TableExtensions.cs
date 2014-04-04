using System;
using System.Data;
using System.Globalization;
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
        /// <exception cref="InvalidOperationException">Another reader of the DBF table is opened.</exception>
        /// <exception cref="ObjectDisposedException">The DBF table is disposed.</exception>
        public static DataTable AsDataTable(this Table table)
        {
            if(table == null)
            {
                throw new ArgumentNullException("table");
            }

            var dataTable = CreateDataTable(table);
            FillData(table, dataTable, table.OpenReader());

            return dataTable;
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

            var dataTable = CreateDataTable(table);
            FillData(table, dataTable, table.OpenReader(encoding));

            return dataTable;
        }

        private static DataTable CreateDataTable(Table table)
        {
            var dataTable = new DataTable()
            {
                Locale = CultureInfo.CurrentCulture
            };

            foreach (var column in table.Columns)
            {
                var columnType = Nullable.GetUnderlyingType(column.Type) ?? column.Type;
                dataTable.Columns.Add(column.Name, columnType);
            }

            return dataTable;
        }

        private static void FillData(Table table, DataTable dataTable, Reader reader)
        {
            while (reader.Read())
            {
                var row = dataTable.NewRow();
                foreach (var column in table.Columns)
                {
                    row[column.Name] = reader.GetValue(column) ?? DBNull.Value;
                }

                dataTable.Rows.Add(row);
            }
        }
    }
}
