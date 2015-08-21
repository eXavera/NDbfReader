using System.IO;

namespace NDbfReader
{
    internal interface IParentTable
    {
        Stream Stream { get; }

        Header Header { get; }

        void ThrowIfDisposed();
    }
}