using System.IO;

namespace NDbfReader
{
    internal interface IParentTable
    {
        void ThrowIfDisposed();

        Header Header { get; }

        BinaryReader BinaryReader { get; }
    }
}
