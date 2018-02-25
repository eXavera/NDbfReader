namespace NDbfReader
{
    /// <summary>
    /// Defines the dBASE date-time formats.
    /// </summary>
    public enum DateTimeNativeFormat
    {
        /// <summary>
        /// Date only. Corresponds to <see cref="NativeColumnType.Date"/>.
        /// </summary>
        Default = 0,

        /// <summary>
        /// FoxPro date and time. Corresponds to <see cref="NativeColumnType.FoxProDateTime"/>.
        /// </summary>
        FoxPro
    }
}