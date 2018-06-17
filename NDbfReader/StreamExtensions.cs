using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace NDbfReader
{
    internal static class StreamExtensions
    {
        public static int ReadBlock(this Stream stream, byte[] buffer, int offset, int count)
        {
            int totalRead = 0;
            int lastRead = 0;

            do
            {
                lastRead = stream.Read(buffer, offset + totalRead, count - totalRead);
                totalRead += lastRead;
            }
            while (totalRead < count && lastRead > 0);

            return totalRead;
        }

        public static async Task<int> ReadBlockAsync(this Stream stream, byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int totalRead = 0;
            int lastRead = 0;

            do
            {
                lastRead = await stream.ReadAsync(buffer, offset + totalRead, count - totalRead, cancellationToken).ConfigureAwait(false);
                totalRead += lastRead;
            }
            while (totalRead < count && lastRead > 0);

            return totalRead;
        }
    }
}
