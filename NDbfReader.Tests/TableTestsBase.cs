namespace NDbfReader.Tests
{
    public abstract class TableTestsBase
    {
        protected static Table GetTableFromEmeddedBasicSample()
        {
            return Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.BASIC));
        }
    }
}