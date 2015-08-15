using System.IO;

namespace NDbfReader
{
    internal interface IParentTable
    {
        BinaryReader BinaryReader { get; }

        Header Header { get; }

        void ThrowIfDisposed();
    }
}