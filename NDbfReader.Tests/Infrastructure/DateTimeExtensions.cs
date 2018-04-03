using System;

namespace NDbfReader.Tests.Infrastructure
{
    internal static class DateTimeExtensions
    {
        public static DateTime? TrimMilliseconds(this DateTime? dateTime)
        {
            if (dateTime.HasValue)
            {
                DateTime value = dateTime.Value;
                return new DateTime(value.Year, value.Month, value.Day, value.Hour, value.Minute, value.Second);
            }

            return null;
        }
    }
}
