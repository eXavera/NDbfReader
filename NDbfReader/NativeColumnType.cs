namespace NDbfReader
{
    /// <summary>
    /// Supported native dBASE column types.
    /// </summary>
    public static class NativeColumnType
    {
        /// <summary>
        /// 8 bytes - date stored as a string in the format YYYYMMDD.
        /// </summary>
        /// <remarks>D in ASCII</remarks>
        public const byte Date = 0x44;

        /// <summary>
        /// Number stored as a string, right justified, and padded with blanks to the width of the field.
        /// </summary>
        /// <remarks>F in ASCII</remarks>
        public const byte Float = 0x46;

        /// <summary>
        /// All OEM code page characters - padded with blanks to the width of the field.
        /// </summary>
        /// <remarks>C in ASCII</remarks>
        public const byte Char = 0x43;

        /// <summary>
        /// 1 byte - initialized to 0x20 (space) otherwise T or F
        /// </summary>
        /// <remarks>L in ASCII</remarks>
        public const byte Logical = 0x4C;

        /// <summary>
        /// 4 bytes. Leftmost bit used to indicate sign, 0 negative.
        /// </summary>
        /// <remarks> I in ASCII</remarks>
        public const byte Long = 0x49;

        /// <summary>
        /// Number stored as a string, right justified, and padded with blanks to the width of the field.
        /// </summary>
        /// <remarks>N in ASCII</remarks>
        public const byte Numeric = 0x4E;
    }
}