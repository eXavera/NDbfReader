using FluentAssertions;
using NSubstitute;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Extensions;

namespace NDbfReader.Tests
{
    public sealed class TableExtensionsTests
    {
        private const string NOTHING = "NOTHING";

        [Fact]
        public void AsDataTable_ReturnsDataTableWithSchemaThatMatchesDbfTableColumns()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var expectedDataTableColumns = Samples.BasicTableContent.Select(pair => new { Name = pair.Key, Type = pair.Value[0].GetType() });

                // Act
                var dataTable = table.AsDataTable();

                // Assert
                var actualDataTableColumns = dataTable.Columns.Cast<DataColumn>().Select(column => new { Name = column.ColumnName, Type = column.DataType });
                actualDataTableColumns.ShouldAllBeEquivalentTo(expectedDataTableColumns);
            }
        }

        [Theory]
        [InlineData("TEXT")]
        [InlineData("NUMERIC")]
        [InlineData("LOGICAL")]
        [InlineData("DATE")]
        [InlineData("LONG")]
        public void AsDataTable_ReturnsPopulatedDataTable(string columnName)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var expectedValues = ReplaceNullWithDBNull(Samples.BasicTableContent[columnName]);

                // Act
                var dataTable = table.AsDataTable();

                // Assert
                var actualValues = dataTable.AsEnumerable().Select(row => row[columnName]);
                actualValues.ShouldAllBeEquivalentTo(expectedValues);
            }
        }

        [Fact]
        public void AsDataTable_NoParameters_OpensReaderWithASCIIEncoding()
        {
            AsDataTable_OpensReaderWithASCIIEncoding(table => table.AsDataTable());
        }

        [Fact]
        public void AsDataTable_ColumnNames_OpensReaderWithASCIIEncoding()
        {
            AsDataTable_OpensReaderWithASCIIEncoding(table => table.AsDataTable("LOGICAL"));
        }

        private void AsDataTable_OpensReaderWithASCIIEncoding(Action<Table> action)
        {
            // Arrange
            using (var table = GetMockedBasicSampleTable())
            {
                // Act
                action(table);

                // Assert
                table.Received().OpenReader(Encoding.ASCII);
            }
        }

        [Fact]
        public void AsDataTable_CustomEncodingAndColumnNames_OpensReaderWithTheEncoding()
        {
            AsDataTable_OpensReaderWithTheEncoding((table, encoding) => table.AsDataTable(encoding, "LOGICAL"));
        }

        [Fact]
        public void AsDataTable_CustomEncoding_OpensReaderWithTheEncoding()
        {
            AsDataTable_OpensReaderWithTheEncoding((table, encoding) => table.AsDataTable(encoding));
        }

        private void AsDataTable_OpensReaderWithTheEncoding(Action<Table, Encoding> action)
        {
            // Arrange
            using (var table = GetMockedBasicSampleTable())
            {
                var encoding = Encoding.UTF8;

                // Act
                action(table, encoding);

                // Assert
                table.Received().OpenReader(encoding);
            }
        }

        [Fact]
        public void AsDataTable_ColumnNames_ReturnsDataTableOnlyWithTheColumnNames()
        {
            // Arrange
            using (var table = Table.Open(Samples.GetBasicTableStream()))
            {
                // Act
                var actualColumns = table.AsDataTable("DATE", "LOGICAL").Columns.Cast<DataColumn>().Select(c => c.ColumnName);

                // Assert
                actualColumns.ShouldAllBeEquivalentTo(new[] { "DATE", "LOGICAL" });
            }
        }

        [Fact]
        public void AsDataTable_InvalidColumnName_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using (var table = Table.Open(Samples.GetBasicTableStream()))
            {
                var invalidColumnName = "DATEE";

                // Act
                var exception = Assert.Throws<ArgumentOutOfRangeException>(() => table.AsDataTable(invalidColumnName));

                // Assert
                Assert.Equal("columnNames", exception.ParamName);
                Assert.Equal(invalidColumnName, exception.ActualValue);
            }
        }

        private static IEnumerable<object> ReplaceNullWithDBNull(IEnumerable<object> items)
        {
            foreach (var item in items)
            {
                yield return item ?? DBNull.Value;
            }
        }

        private static Table GetMockedBasicSampleTable()
        {
            return Substitute.ForPartsOf<MockableTable>(
                new Header(DateTime.Now, 0, 0, new List<IColumn>() { new BooleanColumn("LOGICAL", 0) }),
                new BinaryReader(Samples.GetBasicTableStream()));
        }

        public class MockableTable : Table
        {
            public MockableTable(Header header, BinaryReader reader) : base(header, reader) { }
        }
    }
}
