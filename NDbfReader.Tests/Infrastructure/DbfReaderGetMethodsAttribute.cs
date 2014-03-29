using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit.Extensions;

namespace NDbfReader.Tests.Infrastructure
{
    /// <summary>
    /// Provides GetXX method names and argument types for a data theory.
    /// For parameter of type <see cref="System.String"/> the method name is provided. For parameter of type <see cref="System.Type"/> the type of parameter argument is provided.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    internal sealed class DbfReaderGetMethodsAttribute : DataAttribute
    {
        private readonly string[] _excludedMethods;

        public DbfReaderGetMethodsAttribute(params string[] exclude)
        {
            _excludedMethods = exclude;
        }

        public override IEnumerable<object[]> GetData(MethodInfo methodUnderTest, Type[] parameterTypes)
        {
            var readerType = typeof(Reader);

            var getMethods = readerType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(method => method.Name.StartsWith("Get"))
                .Where(method => !_excludedMethods.Contains(method.Name))
                .Where(method => method.GetParameters().Length == 1);

            if (!parameterTypes.Contains(typeof(Type)))
            {
                //if the method under test doesn't have a Type parameter, assume it expects only method names
                getMethods = getMethods.Distinct(new DelegatedEqualityComparer<MethodInfo>(m => m.Name));
            }

            return getMethods.Select(method =>
            {
                return parameterTypes.Select<Type, object>(parameterType =>
                {
                    if (parameterType == typeof(string)) return method.Name;
                    if (parameterType == typeof(Type)) return method.GetParameters().First().ParameterType;

                    throw new InvalidOperationException(string.Format("Unexpected parameter of type {0}. Only method name and parameter type parameters area supported.", parameterType.FullName));
                })
                .ToArray();
            });
        }
    }
}
