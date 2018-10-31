using System.IO;

namespace NDbfReader.Tests
{
    internal static class EmbeddedSamples
    {
        public const string BASIC = "SupportedTypes.dbf";
        public const string BIG_CZECH_DATA = "BigCzechData.dbf";
        public const string CZECH_ENCODING = "CzechEncoding.dbf";
        public const string DELETED_ROWS = "DeletedRows.dbf";
        public const string UNSUPPORTED_TYPES = "UnsupportedTypes.dbf";
        public const string ZERO_SIZE_COLUMN = "ZeroSizeColumn.dbf";
        public const string WHITE_SPACES = "WhiteSpaces.dbf";
        public const string FOXPRO_DATETIME = "FoxProDateTime.dbf";
        public const string TEN_BYTES_DATES = "10BytesDates.dbf";

        public static Stream GetStream(string fileName)
        {
            var assembly = typeof(EmbeddedSamples).Assembly;
            return assembly.GetManifestResourceStream("NDbfReader.Tests.Samples." + fileName);
        }
    }
}