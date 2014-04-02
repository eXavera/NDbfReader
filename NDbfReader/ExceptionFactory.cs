using System;
using System.Globalization;

namespace NDbfReader
{
    internal static class ExceptionFactory
    {
        public static ArgumentException CreateArgumentException(string paramName, string message, params object[] args)
        {
            return new ArgumentException(string.Format(CultureInfo.InvariantCulture, message, args), paramName);
        }

        public static ArgumentOutOfRangeException CreateArgumentOutOfRangeException(string paramName, string message, params object[] args)
        {
            return new ArgumentOutOfRangeException(paramName, string.Format(CultureInfo.InvariantCulture, message, args));
        }

        public static NotSupportedException CreateNotSupportedException(string message, params object[] args)
        {
            return new NotSupportedException(string.Format(CultureInfo.InvariantCulture, message, args));
        }
    }
}
