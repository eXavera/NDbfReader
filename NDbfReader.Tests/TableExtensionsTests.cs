using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class TableExtensionsTests
    {
        private const string NOTHING = "NOTHING";

        [Theory]
        [InlineDataWithExecMode]
        public async Task AsDataTable_ColumnNames_ReturnsDataTableOnlyWithTheColumnNames(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                // Act
                var actualColumns = (await this.Exec(() => table.AsDataTable("DATE", "LOGICAL"), useAsync))
                    .Columns
                    .Cast<DataColumn>().Select(c => c.ColumnName);

                // Assert
                actualColumns.ShouldAllBeEquivalentTo(new[] { "DATE", "LOGICAL" }, opt => opt.WithStrictOrdering());
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task AsDataTable_CustomEncodingAndEmptyColumnNamesList_ThrowsArgumentException(bool useAsync)
        {
            return AsDataTable_EmptyColumnNamesList_ThrowsArgumentException((table, columns) => this.Exec(() => table.AsDataTable(Encoding.ASCII, columns), useAsync), useAsync);
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task AsDataTable_CustomEncodingAndNoColumnNames_ReturnsDataTableWithAllColumns(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                // Act
                var dataTable = await this.Exec(() => table.AsDataTable(Encoding.ASCII), useAsync);

                // Assert
                Assert.Equal(5, dataTable.Columns.Count);
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task AsDataTable_DefaultEncodingAndEmptyColumnNamesList_ThrowsArgumentException(bool useAsync)
        {
            return AsDataTable_EmptyColumnNamesList_ThrowsArgumentException((table, columns) => this.Exec(() => table.AsDataTable(columns), useAsync), useAsync);
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task AsDataTable_InvalidColumnName_ThrowsArgumentOutOfRangeException(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var invalidColumnName = "DATEE";

                // Act
                var exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => this.Exec(() => table.AsDataTable(invalidColumnName), useAsync));

                // Assert
                Assert.Equal("columnNames", exception.ParamName);
                Assert.Equal(invalidColumnName, exception.ActualValue);
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task AsDataTable_NoParameters_ReturnsDataTableWithAllColumns(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                // Act
                var dataTable = await this.Exec(() => table.AsDataTable(), useAsync);

                // Assert
                Assert.Equal(5, dataTable.Columns.Count);
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task AsDataTable_ReturnsDataTableWithSchemaThatMatchesDbfTableColumns(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var expectedDataTableColumns = Samples.BasicTableContent.Select(pair => new { Name = pair.Key, Type = pair.Value[0].GetType() });

                // Act
                var dataTable = await this.Exec(() => table.AsDataTable(), useAsync);

                // Assert
                var actualDataTableColumns = dataTable.Columns.Cast<DataColumn>().Select(column => new { Name = column.ColumnName, Type = column.DataType });
                actualDataTableColumns.ShouldAllBeEquivalentTo(expectedDataTableColumns, opt => opt.WithStrictOrdering());
            }
        }

        [Theory]
        [InlineDataWithExecMode("TEXT")]
        [InlineDataWithExecMode("NUMERIC")]
        [InlineDataWithExecMode("LOGICAL")]
        [InlineDataWithExecMode("DATE")]
        [InlineDataWithExecMode("LONG")]
        public async Task AsDataTable_ReturnsPopulatedDataTable(bool useAsync, string columnName)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var expectedValues = ReplaceNullWithDBNull(Samples.BasicTableContent[columnName]);

                // Act
                var dataTable = await this.Exec(() => table.AsDataTable(), useAsync);

                // Assert
                var actualValues = dataTable.AsEnumerable().Select(row => row[columnName]);
                actualValues.ShouldAllBeEquivalentTo(expectedValues, opt => opt.WithStrictOrdering());
            }
        }

        private static Table GetMockedBasicSampleTable()
        {
            return Substitute.ForPartsOf<MockableTable>(
                new Header(DateTime.Now, 0, 0, new List<IColumn> { new BooleanColumn("LOGICAL", 0, 1) }),
                Samples.GetBasicTableStream());
        }

        private static IEnumerable<object> ReplaceNullWithDBNull(IEnumerable<object> items)
        {
            foreach (var item in items)
            {
                yield return item ?? DBNull.Value;
            }
        }

        private async Task AsDataTable_EmptyColumnNamesList_ThrowsArgumentException(Func<Table, string[], Task> action, bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                // Act
                var exception = await Assert.ThrowsAsync<ArgumentException>(() => action(table, new string[] { }));

                // Assert
                Assert.Equal("columnNames", exception.ParamName);
                exception.Message.Should().StartWith("No column names specified. Specify at least one column.");
            }
        }

        private Task<Table> OpenBasicTable(bool useAsync)
        {
            return this.Exec(() => Samples.OpenBasicTable(), useAsync);
        }

        public class MockableTable : Table
        {
            public MockableTable(Header header, Stream stream) : base(header, stream)
            {
            }
        }
    }
}