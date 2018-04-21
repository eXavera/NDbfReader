using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace NDbfReader.Tests.Infrastructure
{
    internal sealed class StreamWithPartialReads : Stream
    {
        private readonly Stream _innerStrean;

        public StreamWithPartialReads(Stream innerStrean)
        {
            _innerStrean = innerStrean ?? throw new ArgumentNullException(nameof(innerStrean));
        }

        public override bool CanRead => _innerStrean.CanRead;

        public override bool CanSeek => _innerStrean.CanSeek;

        public override bool CanWrite => _innerStrean.CanWrite;

        public override long Length => _innerStrean.Length;

        public override long Position { get => _innerStrean.Position; set => _innerStrean.Position = value; }

        public override void Flush() => _innerStrean.Flush();

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _innerStrean.Read(buffer, offset, ChangeRequestedCount(count));
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return _innerStrean.BeginRead(buffer, offset, ChangeRequestedCount(count), callback, state);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _innerStrean.ReadAsync(buffer, offset, ChangeRequestedCount(count), cancellationToken);
        }

        public override int EndRead(IAsyncResult asyncResult) => _innerStrean.EndRead(asyncResult);

        public override long Seek(long offset, SeekOrigin origin) => _innerStrean.Seek(offset, origin);

        public override void SetLength(long value) => _innerStrean.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => _innerStrean.Write(buffer, offset, count);

        private static int ChangeRequestedCount(int count)
        {
            if (count > 0)
            {
                if ((count % 4) == 0)
                {
                    return count / 3;
                }
                else if (count > 3)
                {
                    return count - 2;
                }
            }
            return count;
        }
    }
}
