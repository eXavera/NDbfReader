using System;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Xunit;

namespace NDbfReader.Tests.Infrastructure
{
    public sealed class TestMethodExecutionExtensionsTests
    {
        public interface ISUT
        {
            int Add(int a, int b);

            Task<int> AddAsync(int a, int b, CancellationToken cancellationToken = default(CancellationToken));

            void Log(string msg);

            void Log();

            Task LogAsync(string msg, CancellationToken cancellationToken = default(CancellationToken));

            int Sub(int a, int b);
        }

        [Fact]
        public async Task Exec_InstanceMethodAsyncExecMode_CallsSyncMethod()
        {
            int a = 1, b = 2, expectedResult = 3;
            var sut = Substitute.For<ISUT>();
            sut.AddAsync(a, b).Returns(Task.FromResult(expectedResult));

            int result = await sut.Exec(s => s.Add(a, b), useAsync: true);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            sut.Received().AddAsync(a, b);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public async Task Exec_InstanceMethodSyncExecMode_CallsSyncMethod()
        {
            int a = 1, b = 2, expectedResult = 3;
            var sut = Substitute.For<ISUT>();
            sut.Add(a, b).Returns(expectedResult);

            int result = await sut.Exec(s => s.Add(a, b), useAsync: false);

            sut.Received().Add(a, b);
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public async Task Exec_InstanceVoidMethodAsyncExecMode_CallsSyncMethod()
        {
            string msg = "test";
            var sut = Substitute.For<ISUT>();

            await sut.Exec(s => s.Log(msg), useAsync: true);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            sut.Received().LogAsync(msg);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        [Fact]
        public async Task Exec_InstanceVoidMethodSyncExecMode_CallsSyncMethod()
        {
            string msg = "test";
            var sut = Substitute.For<ISUT>();

            await sut.Exec(s => s.Log(msg), useAsync: false);

            sut.Received().Log(msg);
        }

        [Fact]
        public async Task Exec_StaticMethodAsyncExecMode_CallsSyncMethod()
        {
            string arg = "foo";

            string result = await this.Exec(() => StaticSUT.Method(arg), useAsync: true);

            Assert.Equal(arg + "Async", result);
        }

        [Fact]
        public async Task Exec_StaticMethodSyncExecMode_CallsSyncMethod()
        {
            string arg = "foo";

            string result = await this.Exec(() => StaticSUT.Method(arg), useAsync: false);

            Assert.Equal(arg + "Sync", result);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public Task Exec_ThrowsOriginalException(bool useAsync)
        {
            return Assert.ThrowsAsync<InvalidOperationException>(() => this.Exec(() => StaticSUT.Method(), useAsync));
        }

        public static class StaticSUT
        {
            public static string Method(string arg)
            {
                return arg + "Sync";
            }

            public static string Method()
            {
                throw new InvalidOperationException();
            }

            public static Task<string> MethodAsync(string arg, CancellationToken cancellationToken = default(CancellationToken))
            {
                return Task.FromResult(arg + "Async");
            }

            public static Task<string> MethodAsync(CancellationToken cancellationToken = default(CancellationToken))
            {
                throw new InvalidOperationException();
            }
        }
    }
}