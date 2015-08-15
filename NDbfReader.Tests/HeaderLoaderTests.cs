using Xunit;

namespace NDbfReader.Tests
{
    public sealed class HeaderLoaderTests
    {
        [Fact]
        public void DefaultGetter_AlwaysReturnsTheSameInstance()
        {
            Assert.Same(HeaderLoader.Default, HeaderLoader.Default);
        }

        [Fact]
        public void DefaultGetter_ReturnsHeaderLoaderInstance()
        {
            Assert.IsType<HeaderLoader>(HeaderLoader.Default);
        }
    }
}