using System;
using System.IO;
using System.Threading.Tasks;
using NDbfReader.Helpers;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class StreamExtensionsTests
    {
        [Theory]
        [InlineDataWithExecMode]
        public async Task SeekForward_NegativeOffset_ThrowsArgumentOutOfRangeException(bool useAsync)
        {
            // Arrange
            var stream = Substitute.For<Stream>();
            int offset = -2;

            var exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => this.Exec(() => StreamExtensions.SeekForward(stream, offset), useAsync));
            Assert.Equal("offset", exception.ParamName);
        }

        [Theory]
        [InlineDataWithExecMode(100, 1)]
        [InlineDataWithExecMode(100, 3)]
        [InlineDataWithExecMode(100, 20)]
        [InlineDataWithExecMode(100, 60)]
        [InlineDataWithExecMode(2048, 1024)]
        public Task SeekForward_NonSeekableStream(bool useAsync, int capacity, int offset)
        {
            return SeekForward_Stream(useAsync, capacity, offset, stream => stream.CanSeek.Returns(false));
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task SeekForward_NullStream_ThrowsArgumentNullException(bool useAsync)
        {
            Stream stream = null;
            int offset = 10;

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(() => this.Exec(() => StreamExtensions.SeekForward(stream, offset), useAsync));

            Assert.Equal("stream", exception.ParamName);
        }

        [Theory]
        [InlineDataWithExecMode(100, 1)]
        [InlineDataWithExecMode(100, 3)]
        [InlineDataWithExecMode(100, 20)]
        [InlineDataWithExecMode(100, 60)]
        [InlineDataWithExecMode(2048, 1024)]
        public Task SeekForward_SeekableStream(bool useAsync, int capacity, int offset)
        {
            return SeekForward_Stream(useAsync, capacity, offset);
        }

        private async Task SeekForward_Stream(bool useAsync, int capacity, int offset, Action<Stream> setup = null)
        {
            var buffer = new byte[capacity];
            var stream = Substitute.ForPartsOf<MemoryStream>(buffer);
            if (setup != null)
            {
                setup(stream);
            }

            await this.Exec(() => StreamExtensions.SeekForward(stream, offset), useAsync);

            Assert.Equal(offset, stream.Position);
        }
    }
}