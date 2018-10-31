using System;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class ColumnCollectionTests
    {
        [Fact]
        public void Count_ReturnsNumberOfColumns()
        {
            var collection = new ColumnCollection(CreateTestColumns());

            collection.Count.ShouldBeEquivalentTo(2);
        }

        [Fact]
        public void Ctor_Null_ThrowsArgumentNullException()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new ColumnCollection(null));
            exception.ParamName.ShouldBeEquivalentTo("columns");
        }

        [Fact]
        public void ImplementsIEnumerable()
        {
            IColumn[] testColumns = CreateTestColumns();
            IEnumerable<IColumn> collection = new ColumnCollection(testColumns);

            collection.ShouldAllBeEquivalentTo(testColumns, opt => opt.WithStrictOrdering());
        }

        [Fact]
        public void Indexer_ColumnName_ReturnsMatchingColumn()
        {
            IColumn[] testColumns = CreateTestColumns();
            var collection = new ColumnCollection(testColumns);
            IColumn secondColumn = testColumns[1];

            collection[secondColumn.Name].Should().BeSameAs(secondColumn);
        }

        [Fact]
        public void Indexer_Index_ReturnsColumnAtTheIndex()
        {
            IColumn[] testColumns = CreateTestColumns();
            var collection = new ColumnCollection(testColumns);

            collection[1].Should().BeSameAs(testColumns[1]);
        }

        [Fact]
        public void Indexer_InvalidColumnName_ThrowsArgumentOutOfRangeException()
        {
            var collection = new ColumnCollection(CreateTestColumns());

            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => collection["foo"]);
            exception.ParamName.ShouldBeEquivalentTo("columnName");
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(3)]
        public void Indexer_InvalidIndex_ThrowsArgumentOutOfRangeException(int index)
        {
            IColumn[] testColumns = CreateTestColumns();
            var collection = new ColumnCollection(testColumns);

            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => collection[index]);
            exception.ParamName.ShouldBeEquivalentTo("index");
        }

        [Fact]
        public void Indexer_NullColumnName_ThrowsArgumentNullException()
        {
            var collection = new ColumnCollection(CreateTestColumns());

            var exception = Assert.Throws<ArgumentNullException>(() => collection[null]);
            exception.ParamName.ShouldBeEquivalentTo("columnName");
        }

        private static IColumn[] CreateTestColumns()
        {
            return new IColumn[] { new BooleanColumn("col1", 10, 1), new DateTimeColumn("col2", 20, 8) };
        }
    }
}