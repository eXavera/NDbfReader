using System.IO;

namespace NDbfReader
{
    internal interface IParentTable
    {
        Header Header { get; }

        Stream Stream { get; }

        void ThrowIfDisposed();
    }
}