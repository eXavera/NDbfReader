using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class TableTests
    {
        [Theory]
        [InlineDataWithExecMode]
        public Task Columns_DisposedInstance_ThrowsObjectDisposedException(bool useAsync)
        {
            return PublicInterfaceInteraction_DisposedInstance_ThrowsObjectDisposedException(useAsync, table => table.Columns);
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task Columns_ReturnsColumnsNames(bool useAsync)
        {
            return Columns_LoadedFile_ReturnsColumnsProperties(useAsync, column => column.Name, "TEXT", "DATE", "NUMERIC", "LOGICAL", "LONG");
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task Columns_ReturnsColumnsSizes(bool useAsync)
        {
            return Columns_LoadedFile_ReturnsColumnsProperties(useAsync, column => column.Size, 100, 8, 18, 1, 4);
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task Columns_ReturnsColumnsTypes(bool useAsync)
        {
            return Columns_LoadedFile_ReturnsColumnsProperties(useAsync, column => column.Type, typeof(string), typeof(DateTime?), typeof(decimal?), typeof(bool?), typeof(int));
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Dispose_DisposedInstance_DoesNotDisposeTheBaseStream(bool useAsync)
        {
            // Arrange
            var streamSpy = Spy.OnStream(Samples.GetBasicTableStream());
            var table = await this.Exec(() => Table.Open(streamSpy), useAsync);

            // Act
            table.Dispose();
            table.Dispose();

            // Assert
            streamSpy.Received(requiredNumberOfCalls: 1).Dispose();
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Dispose_LoadedFromStream_DisposesTheStream(bool useAsync)
        {
            // Arrange
            var streamSpy = Spy.OnStream(Samples.GetBasicTableStream());
            var table = await this.Exec(() => Table.Open(streamSpy), useAsync);

            // Act
            table.Dispose();

            // Assert
            streamSpy.Received().Dispose();
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task LastModified_ReturnsDateOfLastModification(bool useAsync)
        {
            // Arrange
            using (Table table = await OpenBasicTable(useAsync))
            {
                // Act
                var lastModified = table.LastModified;

                // Assert
                Assert.Equal(new DateTime(2014, 2, 20), lastModified);
            }
        }

        [Fact]
        public void Open_CustomHeaderLoader_LoadsHeaderWithTheLoader()
        {
            // Arrange
            var headerLoader = Substitute.ForPartsOf<HeaderLoader>();

            // Act
            using (var table = Table.Open(Samples.GetBasicTableStream(), headerLoader)) { }

            // Assert
            headerLoader.Received().Load(Arg.Any<Stream>());
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task Open_DisposedStream_ThrowsArgumentException(bool useAsync)
        {
            // Arrange
            var disposedStream = new MemoryStream();
            disposedStream.Dispose();

            // Act & Assert
            return Assert.ThrowsAsync<ArgumentException>(() => this.Exec(() => Table.Open(disposedStream), useAsync));
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Open_NullStream_ThrowsArgumentNullException(bool useAsync)
        {
            Stream stream = null;

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(() => this.Exec(() => Table.Open(stream), useAsync));

            Assert.Equal("stream", exception.ParamName);
        }

        [Theory]
        [InlineDataWithExecMode(true)]
        [InlineDataWithExecMode(false)]
        public async Task Open_TableWithExtraHeaderFields_ReturnsCorrectValues(bool useAsync, bool seekableStream)
        {
            Stream tableStream = EmbeddedSamples.GetStream(EmbeddedSamples.UNSUPPORTED_TYPES);
            if (seekableStream)
            {
                tableStream = tableStream.ToNonSeekable();
            }

            using (var table = await this.Exec(() => Table.Open(tableStream), useAsync))
            {
                var reader = table.OpenReader();
                await reader.Exec(r => r.Read(), useAsync);

                var expectedValues = new object[] { 1, "Chai", 1, 1, "10 boxes x 20 bags" };
                var actualValues = new object[] { reader.GetValue("PRODUCTID"), reader.GetValue("PRODUCTNAM"), reader.GetValue("SUPPLIERID"), reader.GetValue("CATEGORYID"), reader.GetValue("QUANTITYPE") };

                actualValues.ShouldAllBeEquivalentTo(expectedValues, opt => opt.WithStrictOrdering());
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Open_UnreadableStream_ThrowsArgumentException(bool useAsync)
        {
            var unreadableStream = Substitute.For<Stream>();
            unreadableStream.CanRead.Returns(false);

            var exception = await Assert.ThrowsAsync<ArgumentException>(() => this.Exec(() => Table.Open(unreadableStream), useAsync));

            exception.ParamName.Should().BeEquivalentTo("stream");
            exception.Message.Should().StartWithEquivalent($"The stream does not allow reading ({nameof(unreadableStream.CanRead)} property returns false).");
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Open_UnsupportedColumnType_CreatesRawColumnInstance(bool useAsync)
        {
            using (var table = await this.Exec(() => Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.UNSUPPORTED_TYPES)), useAsync))
            {
                var actualUnsupportedColumns = table.Columns.OfType<RawColumn>().Select(c => new { c.Name, c.NativeType });

                var expectedColumns = new[] { new { Name = "UNITPRICE", NativeType = 89 }, new { Name = "_NullFlags", NativeType = 48 } };
                actualUnsupportedColumns.ShouldAllBeEquivalentTo(expectedColumns, opt => opt.WithStrictOrdering());
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Open_FoxProDateTimeColumn_CreatesFoxProDateTimeColumnInstance(bool useAsync)
        {
            var tableStream = EmbeddedSamples.GetStream(EmbeddedSamples.FOXPRO_DATETIME);
            using (var table = await this.Exec(() => Table.Open(tableStream), useAsync))
            {
                var foxProDateTimeColumn = table.Columns[Samples.FoxProDateTime.FirstRow().columnName];

                foxProDateTimeColumn.Should().BeOfType<FoxProDateTimeColumn>();
            }
        }

        [Fact]
        public async Task OpenAsync_CustomHeaderLoader_LoadsHeaderWithTheLoader()
        {
            // Arrange
            var headerLoader = Substitute.ForPartsOf<HeaderLoader>();

            // Act
            using (var table = await Table.OpenAsync(Samples.GetBasicTableStream(), headerLoader)) { }

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            // Assert
            headerLoader.Received().LoadAsync(Arg.Any<Stream>());
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task OpenReader_AnotherReaderIsAlreadyOpened_ThrowsInvalidOperationException(bool useAsync)
        {
            // Arrange
            var table = await OpenBasicTable(useAsync);
            table.OpenReader();

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => table.OpenReader());
            Assert.Equal("The table can open only one reader.", exception.Message);
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task OpenReader_DisposedInstance_ThrowsObjectDisposedException(bool useAsync)
        {
            return PublicInterfaceInteraction_DisposedInstance_ThrowsObjectDisposedException(useAsync, table => table.OpenReader());
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task OpenReader_NullEncoding_ThrowsArgumentNullException(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                // Act & Assert
                var exception = Assert.Throws<ArgumentNullException>(() => table.OpenReader(encoding: null));
                Assert.Equal("encoding", exception.ParamName);
            }
        }

        private async Task Columns_LoadedFile_ReturnsColumnsProperties<T>(bool useAsync, Func<IColumn, T> propertySelector, params T[] expectedValues)
        {
            // Act
            ColumnCollection actualColumns = null;

            using (var table = await OpenBasicTable(useAsync))
            {
                actualColumns = table.Columns;
            }

            // Assert
            actualColumns.Select(propertySelector).ShouldAllBeEquivalentTo(expectedValues, opt => opt.WithStrictOrdering());
        }

        private Task<Table> OpenBasicTable(bool useAsync)
        {
            return this.Exec(() => Samples.OpenBasicTable(), useAsync);
        }

        private async Task PublicInterfaceInteraction_DisposedInstance_ThrowsObjectDisposedException(bool useAsync, Func<Table, object> action)
        {
            // Arrange
            var table = await OpenBasicTable(useAsync);
            table.Dispose();

            // Act & Assert
            var exception = Assert.Throws<ObjectDisposedException>(() => action(table));
            Assert.Equal(typeof(Table).FullName, exception.ObjectName);
        }
    }
}