using System.Text;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class AdditionalReaderTests
    {
        [Fact]
        public void ReadsBigCzechSampleTableWithoutErrors()
        {
            // Arrange & Act
            using (var table = Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.BIG_CZECH_DATA)))
            {
                var readRows = 0;
                var reader = table.OpenReader(Encoding.GetEncoding(1250));

                object value = null;
                while (reader.Read())
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
