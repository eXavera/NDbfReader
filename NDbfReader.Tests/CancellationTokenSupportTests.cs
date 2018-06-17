using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class CancellationTokenSupportTests
    {
        [Fact]
        public Task OpenAsync_CancellationToken_ReadsAllBytesUsingTheToken()
        {
            return Action_CancellationToken_ReadsAllBytesUsingTheToken((stream, cancellationToken) =>
            {
                return Table.OpenAsync(stream, HeaderLoader.Default, cancellationToken);
            });
        }

        [Fact]
        public Task AsDataTableAsync_CancellationToken_ReadsAllBytesUsingTheToken()
        {
            return Action_CancellationToken_ReadsAllBytesUsingTheToken(async (stream, cancellationToken) =>
            {
                using (var table = await Table.OpenAsync(stream, cancellationToken))
                {
                    await table.AsDataTableAsync(cancellationToken);
                }
            });
        }

        [Fact]
        public Task AsDataTableAsync_EncodingAndCancellationToken_ReadsAllBytesUsingTheToken()
        {
            return Action_CancellationToken_ReadsAllBytesUsingTheToken(async (stream, cancellationToken) =>
            {
                using (var table = await Table.OpenAsync(stream, cancellationToken))
                {
                    await table.AsDataTableAsync(Encoding.UTF8, cancellationToken);
                }
            });
        }

        [Fact]
        public Task AsDataTableAsync_ColumnNameAndCancellationToken_ReadsAllBytesUsingTheToken()
        {
            return Action_CancellationToken_ReadsAllBytesUsingTheToken(async (stream, cancellationToken) =>
            {
                using (var table = await Table.OpenAsync(stream, cancellationToken))
                {
                    await table.AsDataTableAsync(cancellationToken, "TEXT");
                }
            });
        }

        [Fact]
        public Task AsDataTableAsync_EncodingAndColumnNameAndCancellationToken_ReadsAllBytesUsingTheToken()
        {
            return Action_CancellationToken_ReadsAllBytesUsingTheToken(async (stream, cancellationToken) =>
            {
                using (var table = await Table.OpenAsync(stream, cancellationToken))
                {
                    await table.AsDataTableAsync(Encoding.UTF8, cancellationToken, "TEXT");
                }
            });
        }

        [Fact]
        public Task ReadAsync_CancellationToken_ReadsAllBytesUsingTheToken()
        {
            return Action_CancellationToken_ReadsAllBytesUsingTheToken(async (stream, cancellationToken) =>
            {
                using (var table = await Table.OpenAsync(stream, cancellationToken))
                {
                    var reader = table.OpenReader();
                    while (await reader.ReadAsync(cancellationToken))
                    {
                    }
                }
            });
        }

        private async Task Action_CancellationToken_ReadsAllBytesUsingTheToken(Func<Stream, CancellationToken, Task> action)
        {
            var cts = new CancellationTokenSource();
            var streamSpy = Spy.OnStream(Samples.GetBasicTableStream());

            await action(streamSpy, cts.Token);

            await streamSpy.DidNotReceive().ReadAsync(Arg.Any<byte[]>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Is<CancellationToken>(t => t != cts.Token));
        }
    }
}
