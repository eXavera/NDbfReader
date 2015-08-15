using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class TableTests
    {
        [Fact]
        public void Columns_DisposedInstance_ThrowsObjectDisposedException()
        {
            PublicInterfaceInteraction_DisposedInstance_ThrowsObjectDisposedException(table => table.Columns);
        }

        [Fact]
        public void Columns_ReturnsColumnsNames()
        {
            Columns_LoadedFile_ReturnsColumnsProperties(column => column.Name, "TEXT", "NUMERIC", "DATE", "LONG", "LOGICAL");
        }

        [Fact]
        public void Columns_ReturnsColumnsTypes()
        {
            Columns_LoadedFile_ReturnsColumnsProperties(column => column.Type, typeof(string), typeof(decimal?), typeof(DateTime?), typeof(int), typeof(bool?));
        }

        [Fact]
        public void Dispose_DisposedInstance_DoesNotDisposeTheBaseStream()
        {
            // Arrange
            var streamSpy = Spy.OnStream(Samples.GetBasicTableStream());
            var table = Table.Open(streamSpy);

            // Act
            table.Dispose();
            table.Dispose();

            // Assert
            streamSpy.Received(requiredNumberOfCalls: 1).Dispose();
        }

        [Fact]
        public void Dispose_LoadedFromStream_DisposesTheStream()
        {
            // Arrange
            var streamSpy = Spy.OnStream(Samples.GetBasicTableStream());
            var table = Table.Open(streamSpy);

            // Act
            table.Dispose();

            // Assert
            streamSpy.Received().Dispose();
        }

        [Fact]
        public void LastModified_ReturnsDateOfLastModification()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
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
            headerLoader.Received().Load(Arg.Any<BinaryReader>());
        }

        [Fact]
        public void Open_DisposedStream_ThrowsArgumentException()
        {
            // Arrange
            var disposedStream = new MemoryStream();
            disposedStream.Dispose();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => Table.Open(disposedStream));
        }

        [Fact]
        public void Open_NullStream_ThrowsArgumentNullException()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() => Table.Open(stream: null));
            Assert.Equal("stream", exception.ParamName);
        }

        [Fact]
        public void Open_TableWithUnsupportedColumn_ThrowsNotSupportedException()
        {
            // Act && Assert
            var exception = Assert.Throws<NotSupportedException>(() => Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.UNSUPPORTED_TYPES)));
            Assert.Equal("The TIMESTAMP column's type is not supported.", exception.Message);
        }

        [Fact]
        public void Open_UnreadableStream_ThrowsArgumentException()
        {
            //Arrange
            var unreadableStream = Substitute.For<Stream>();
            unreadableStream.CanRead.Returns(false);

            //Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => Table.Open(unreadableStream));
            exception.ParamName.Should().BeEquivalentTo("stream");
            exception.Message.Should().StartWithEquivalent($"The stream does not allow reading ({nameof(unreadableStream.CanRead)} property returns false).");
        }

        [Fact]
        public void OpenReader_AnotherReaderIsAlreadyOpened_ThrowsInvalidOperationException()
        {
            // Arrange
            var table = Samples.OpenBasicTable();
            table.OpenReader();

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => table.OpenReader());
            Assert.Equal("The table can open only one reader.", exception.Message);
        }

        [Fact]
        public void OpenReader_DisposedInstance_ThrowsObjectDisposedException()
        {
            PublicInterfaceInteraction_DisposedInstance_ThrowsObjectDisposedException(table => table.OpenReader());
        }

        [Fact]
        public void OpenReader_NullEncoding_ThrowsArgumentNullException()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                // Act & Assert
                var exception = Assert.Throws<ArgumentNullException>(() => table.OpenReader(null));
                Assert.Equal("encoding", exception.ParamName);
            }
        }

        private void Columns_LoadedFile_ReturnsColumnsProperties<T>(Func<IColumn, T> propertySelector, params T[] expectedValues)
        {
            // Act
            ReadOnlyCollection<IColumn> actualColumns = null;

            using (var table = Samples.OpenBasicTable())
                actualColumns = table.Columns;

            // Assert
            actualColumns.Select(propertySelector).ShouldAllBeEquivalentTo(expectedValues);
        }

        private void PublicInterfaceInteraction_DisposedInstance_ThrowsObjectDisposedException(Func<Table, object> action)
        {
            // Arrange
            var table = Samples.OpenBasicTable();
            table.Dispose();

            // Act & Assert
            var exception = Assert.Throws<ObjectDisposedException>(() => action(table));
            Assert.Equal(typeof(Table).FullName, exception.ObjectName);
        }
    }
}