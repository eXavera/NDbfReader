using NSubstitute;
using System.IO;

namespace NDbfReader.Tests.Infrastructure
{
    internal static class Spy
    {
        public static Stream OnStream(Stream stream)
        {
            return Substitute.ForPartsOf<StreamDecorator>(stream);
        }
    }
}
