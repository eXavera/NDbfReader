using NSubstitute;
using NSubstitute.Core;
using System;
using System.IO;
using Xunit;
using Xunit.Extensions;

namespace NDbfReader.Tests
{
    public sealed class StreamExtensionsTests
    {
        [Fact]
        public void SeekForward_NullStream_ThrowsArgumentNullException()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => StreamExtensions.SeekForward(null, 10));
            Assert.Equal("stream", exception.ParamName);
        }

        [Fact]
        public void SeekForward_NegativeOffset_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var stream = Substitute.For<Stream>();

            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => StreamExtensions.SeekForward(stream, -2));
            Assert.Equal("offset", exception.ParamName);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(20)]
        public void SeekForward_SeekableStream_CallsSeekMethodWithCurrentSeekOrigin(int offset)
        {
            // Arrange
            var stream = Substitute.For<Stream>();
            stream.CanSeek.Returns(true);

            // Act
            StreamExtensions.SeekForward(stream, offset);

            // Assert
            stream.Received(requiredNumberOfCalls: 1).Seek(offset, SeekOrigin.Current);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(20)]
        public void SeekForward_NonSeekableStream_NotCallsSeekMethod(int offset)
        {
            // Arrange
            var stream = Substitute.For<Stream>();
            stream.CanSeek.Returns(false);

            // Act
            StreamExtensions.SeekForward(stream, offset);

            // Assert
            stream.DidNotReceive().Seek(Arg.Any<long>(), Arg.Any<SeekOrigin>());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void SeekForward_SmallOffsetOnNonSeekableStream_CallsReadByteMethodForEveryByteOfTheOffset(int offset)
        {
            // Arrange
            var stream = Substitute.For<Stream>();
            stream.CanSeek.Returns(false);

            // Act
            StreamExtensions.SeekForward(stream, offset);

            // Assert
            stream.Received(requiredNumberOfCalls: offset).ReadByte();
        }

        [Theory]
        [InlineData(4)]
        [InlineData(10)]
        [InlineData(100)]
        public void SeekForward_NormalOffsetOnNonSeekableStream_ReadsEntireOffsetIntoSingleBuffer(int offset)
        {
            // Arrange
            var stream = Substitute.For<Stream>();
            stream.CanSeek.Returns(false);

            // Act
            StreamExtensions.SeekForward(stream, offset);

            // Assert
            stream.Received(requiredNumberOfCalls: 1).Read(Arg.Any<byte[]>(), 0, offset);
        }

        [Fact]
        public void SeekForward_LargeOffsetOnNonSeekableStream_RepeatedlyReadsTheOffsetInSmallerChunks()
        {
            // Arrange
            var stream = Substitute.For<Stream>();
            stream.CanSeek.Returns(false);
            stream.Read(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>()).Returns(CountArgument);

            // Act
            StreamExtensions.SeekForward(stream, 600);

            // Assert
            Received.InOrder(() =>
            {
                stream.Received().Read(Arg.Any<byte[]>(), Arg.Any<int>(), 255);
                stream.Received().Read(Arg.Any<byte[]>(), Arg.Any<int>(), 255);
                stream.Received().Read(Arg.Any<byte[]>(), Arg.Any<int>(), 90);
            });
        }

        private static object CountArgument(CallInfo callInfo)
        {
            return callInfo.Args()[2];
        }
    }
}
