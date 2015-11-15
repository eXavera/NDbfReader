using System.Text;
using System.Threading.Tasks;
using NDbfReader.Tests.Infrastructure;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class AdditionalReaderTests
    {
        [Theory]
        [InlineDataWithExecMode]
        public async Task ReadsBigCzechSampleTableWithoutErrors(bool useAsync)
        {
            // Arrange & Act
            using (var table = await this.Exec(() => Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.BIG_CZECH_DATA)), useAsync))
            {
                var readRows = 0;
                var reader = table.OpenReader(Encoding.GetEncoding(1250));

                object value = null;
                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    foreach (var column in table.Columns)
                    {
                        value = reader.GetValue(column);
                    }

                    readRows++;
                }

                // Assert
                Assert.Equal(1071, readRows);
            }
        }
    }
}