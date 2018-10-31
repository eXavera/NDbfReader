using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class TrailingWhiteSpaceDatesSupportTests
    {
        [Theory]
        [InlineDataWithExecMode]
        public async Task TableWithTenBytesDates_ReadsTheTableProperly(bool useAsync)
        {
            using (var tableStream = EmbeddedSamples.GetStream(EmbeddedSamples.TEN_BYTES_DATES))
            using (var table = await this.Exec(() => Table.Open(tableStream), useAsync))
            {
                var reader = table.OpenReader();
                await reader.Exec(r => r.Read(), useAsync);

                var firstRowContent = table.Columns.Select(reader.GetValue).ToList();

                firstRowContent.ShouldAllBeEquivalentTo(Samples.TenBytesDates.FirstRowContent);
            }
        }
    }
}
