using NSubstitute;
using System;
using System.IO;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class StreamExtensionsTests
    {
        [Fact]
        public void SeekForward_NegativeOffset_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var stream = Substitute.For<Stream>();

            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => StreamExtensions.SeekForward(stream, -2));
            Assert.Equal("offset", exception.ParamName);
        }

        [Theory]
        [InlineData(100, 1)]
        [InlineData(100, 3)]
        [InlineData(100, 20)]
        [InlineData(100, 60)]
        [InlineData(2048, 1024)]
        public void SeekForward_NonSeekableStream(int capacity, int offset)
        {
            SeekForward_Stream(capacity, offset, stream => stream.CanSeek.Returns(false));
        }

        [Fact]
        public void SeekForward_NullStream_ThrowsArgumentNullException()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => StreamExtensions.SeekForward(null, 10));
            Assert.Equal("stream", exception.ParamName);
        }

        [Theory]
        [InlineData(100, 1)]
        [InlineData(100, 3)]
        [InlineData(100, 20)]
        [InlineData(100, 60)]
        [InlineData(2048, 1024)]
        public void SeekForward_SeekableStream(int capacity, int offset)
        {
            SeekForward_Stream(capacity, offset);
        }

        private void SeekForward_Stream(int capacity, int offset, Action<Stream> setup = null)
        {
            var buffer = new byte[capacity];
            var stream = Substitute.ForPartsOf<MemoryStream>(buffer);
            if (setup != null)
            {
                setup(stream);
            }

            StreamExtensions.SeekForward(stream, offset);

            Assert.Equal(offset, stream.Position);
        }
    }
}