using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit.Sdk;

namespace NDbfReader.Tests.Infrastructure
{
    /// <summary>
    /// Provides GetXX method names and argument types for a data theory.
    /// For parameter of type <see cref="string"/> the method name is provided. For parameter of type <see cref="Type"/> the type of parameter argument is provided.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    internal sealed class ReaderGetMethodsAttribute : DataAttribute
    {
        private readonly string[] _excludedMethods;

        public ReaderGetMethodsAttribute(params string[] exclude)
        {
            _excludedMethods = exclude;
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            var readerType = typeof(Reader);

            var getMethods = readerType.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.Name.StartsWith("Get", StringComparison.Ordinal))
                .Where(method => !_excludedMethods.Contains(method.Name));

            var parameterTypes = testMethod.GetParameters().Select(p => p.ParameterType).ToList();
            if (!parameterTypes.Contains(typeof(Type)))
            {
                //if the method under test doesn't have a Type parameter, assume it expects only method names
                getMethods = getMethods.Distinct(new DelegatedEqualityComparer<MethodInfo>(m => m.Name));
            }
            if(parameterTypes.Contains(typeof(bool)))
            {
                // useAsync parameter is required, all method are executed twice
                getMethods = DoubleItems(getMethods);
            }

            return getMethods.Select((method, index) =>
            {
                return parameterTypes.Select<Type, object>(parameterType =>
                {
                    if (parameterType == typeof(bool)) return index % 2 == 0; // useAsync
                    if (parameterType == typeof(string)) return method.Name;
                    if (parameterType == typeof(Type)) return method.GetParameters().First().ParameterType;

                    throw new InvalidOperationException($"Unexpected parameter of type {parameterType.FullName}. Test method must expect useAsync, methodName and methodType parameters.");
                })
                .ToArray();
            });
        }

        private static IEnumerable<MethodInfo> DoubleItems(IEnumerable<MethodInfo> methods)
        {
            foreach (MethodInfo method in methods)
            {
                yield return method;
                yield return method;
            }
        }
    }
}