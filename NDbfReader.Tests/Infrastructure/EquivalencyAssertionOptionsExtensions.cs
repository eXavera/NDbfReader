using FluentAssertions.Equivalency;

namespace NDbfReader.Tests.Infrastructure
{
    internal static class EquivalencyAssertionOptionsExtensions
    {
        public static EquivalencyAssertionOptions<TSubject> ExcludingNonPublicProperties<TSubject>(this EquivalencyAssertionOptions<TSubject> options)
        {
            return options.Excluding(o => o.PropertyInfo.GetGetMethod(false/*nonPublic*/) == null);
        }
    }
}
