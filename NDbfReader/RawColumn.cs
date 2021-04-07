using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
    /// <summary>
    /// Represents a raw column. 
    /// </summary>
    [DebuggerDisplay("Raw {NativeTypeHex} {Name}")]
    public sealed class RawColumn : Column<byte[]>
    {
        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="offset">The column offset in a row.</param>
        /// <param name="size">The column size in bytes.</param>
        /// <param name="decimalPrecision">The column decimal precision.</param>
        /// <param name="nativeType">The column's native type code.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
        public RawColumn(string name, int offset, int size, int decimalPrecision, byte nativeType) : base(name, offset, size, decimalPrecision)
        {
            NativeType = nativeType;
        }

        /// <summary>
        /// Loads a value from the specified buffer.
        /// </summary>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="offset">The offset in <paramref name="buffer"/> at which to begin loading the value.</param>
        /// <param name="encoding">The encoding to use to parse the value.</param>
        /// <returns>A column value.</returns>
        protected override byte[] DoLoad(byte[] buffer, int offset, Encoding encoding)
        {
            var result = new byte[Size];
            Array.Copy(buffer, offset, result, 0, result.Length);

            return result;
        }

        /// <summary>
        /// Gets the code of dBASE native type.
        /// </summary>
        public byte NativeType { get; }

        private string NativeTypeHex => $"0x{NativeType:X2}";
    }
}