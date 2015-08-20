using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit.Sdk;

namespace NDbfReader.Tests.Infrastructure
{
    public sealed class InlineDataWithExecModeAttribute : DataAttribute
    {
        private readonly object[] _data;

        public InlineDataWithExecModeAttribute(params object[] data)
        {
            _data = data;
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { CreateMethodArgs(useAsync: false), CreateMethodArgs(useAsync: true) };
        }

        private object[] CreateMethodArgs(bool useAsync)
        {
            object[] args = new object[_data.Length + 1];
            Array.Copy(_data, 0, args, 1, _data.Length);
            args[0] = useAsync;

            return args;
        }
    }
}