using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace NDbfReader.Tests
{
    internal static class Samples
    {
        public static readonly Dictionary<string, List<object>> BasicTableContent = new Dictionary<string, List<object>>
        {
            { "TEXT", new List<object> {"text 1 text", "text 2 text", null} },
            { "DATE", new List<object> { new DateTime(2014, 2, 20), new DateTime(1998, 8, 15), null} },
            { "NUMERIC", new List<object> {123.123m, 456.456m, null} },
            { "LOGICAL", new List<object> {true, false, false} },
            { "LONG", new List<object> {  123456, -6544321, 0} }
        };

        public static Stream GetBasicTableStream()
        {
            return EmbeddedSamples.GetStream(EmbeddedSamples.BASIC);
        }

        public static Table OpenBasicTable()
        {
            return Table.Open(GetBasicTableStream());
        }

        public static Task<Table> OpenBasicTableAsync()
        {
            return Table.OpenAsync(GetBasicTableStream());
        }
    }
}