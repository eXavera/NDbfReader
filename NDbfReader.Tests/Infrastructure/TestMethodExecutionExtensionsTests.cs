using NSubstitute;
using System;
using System.Threading.Tasks;
using Xunit;

namespace NDbfReader.Tests.Infrastructure
{
    public sealed class TestMethodExecutionExtensionsTests
    {
        public interface ISUT
        {
            int Add(int a, int b);

            Task<int> AddAsync(int a, int b);

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

            public static Task<string> MethodAsync(string arg)
            {
                return Task.FromResult(arg + "Async");
            }

            public static Task<string> MethodAsync()
            {
                throw new InvalidOperationException();
            }
        }
    }
}