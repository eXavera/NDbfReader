using FluentAssertions;
using NSubstitute;
using System;
using System.Collections.Generic;
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
            // Arrange
            using (var table = GetMockedBasicSampleTable())
            {
                // Act
                table.AsDataTable();

                // Assert
                table.Received().OpenReader(Encoding.ASCII);
            }
        }

        [Fact]
        public void AsDataTable_CustomEncoding_OpensReaderWithTheEncoding()
        {
            // Arrange
            using (var table = GetMockedBasicSampleTable())
            {
                var encoding = Encoding.UTF8;

                // Act
                table.AsDataTable(encoding);

                // Assert
                table.Received().OpenReader(encoding);
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
            return Substitute.ForPartsOf<MockableTable>(new Header(new List<IColumn>(), 0, 0), new BinaryReader(Samples.GetBasicTableStream()));
        }

        public class MockableTable : Table
        {
            public MockableTable(Header header, BinaryReader reader) : base(header, reader) { }
        }
    }
}
