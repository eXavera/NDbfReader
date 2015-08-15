using System;
using System.Collections.Generic;

namespace NDbfReader.Tests.Infrastructure
{
    internal sealed class DelegatedEqualityComparer<T> : IEqualityComparer<T>
    {
        private readonly Func<T, object> _discriminatorSelector;

        public DelegatedEqualityComparer(Func<T, object> discriminatorSelector)
        {
            if (discriminatorSelector == null) throw new ArgumentNullException(nameof(discriminatorSelector));

            _discriminatorSelector = discriminatorSelector;
        }

        public bool Equals(T x, T y)
        {
            var xValue = _discriminatorSelector(x);
            var yValue = _discriminatorSelector(y);

            if (xValue == null && yValue == null) return true;
            if (xValue == null || yValue == null) return false;

            return xValue.Equals(yValue);
        }

        public int GetHashCode(T obj)
        {
            return _discriminatorSelector(obj).GetHashCode();
        }
    }
}