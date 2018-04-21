using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class NetworkStreamSupportTests
    {
        [Theory]
        [InlineDataWithExecMode]
        public async Task StreamWithPartialReads_ReadsTheTableProperly(bool useAsync)
        {
            using (var tableStream = Samples.GetBasicTableStream().EmulatePartialReads())
            using (var table = await this.Exec(() => Table.Open(tableStream), useAsync))
            {
                var reader = table.OpenReader();

                var actualTableContent = new Dictionary<string, List<object>>();
                foreach (var column in table.Columns)
                {
                    actualTableContent.Add(column.Name, new List<object>());
                }

                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    foreach (var column in table.Columns)
                    {
                        actualTableContent[column.Name].Add(reader.GetValue(column));
                    }
                }

                actualTableContent.ShouldAllBeEquivalentTo(Samples.BasicTableContent);
            }
        }
    }
}