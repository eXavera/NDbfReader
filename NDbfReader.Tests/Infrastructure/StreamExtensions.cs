using System;
using System.IO;
using NSubstitute;

namespace NDbfReader.Tests.Infrastructure
{
    internal static class StreamExtensions
    {
        public static Stream ToNonSeekable(this Stream stream)
        {
            Stream streamSpy = Spy.OnStream(stream);
            streamSpy.CanSeek.Returns(false);
            streamSpy.Seek(Arg.Any<long>(), Arg.Any<SeekOrigin>()).Returns(x => { throw new NotSupportedException(); });
            streamSpy.Position.Returns(x => { throw new NotSupportedException(); });
            streamSpy.When(s => s.Position = Arg.Any<long>()).Do(x => { throw new NotSupportedException(); });

            return streamSpy;
        }

        public static Stream EmulatePartialReads(this Stream stream)
        {
            return new StreamWithPartialReads(stream);
        }
    }
}