using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace NDbfReader.Tests.Infrastructure
{
    public sealed class InlineDataWithExecModeAttributeTests
    {
        [Fact]
        public void GetData_ReturnsDoubledInitialArrayWithSyncAndAsyncValues()
        {
            var attribute = new InlineDataWithExecModeAttribute(1, 2);

            IEnumerable<object[]> result = attribute.GetData(null);

            result.ShouldAllBeEquivalentTo(new[]
            {
                new object[] { false, 1, 2},
                new object[] { true, 1, 2}
            }, opt => opt.WithStrictOrdering());
        }
    }
}