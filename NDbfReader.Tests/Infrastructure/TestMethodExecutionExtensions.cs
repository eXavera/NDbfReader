using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace NDbfReader.Tests.Infrastructure
{
    internal static class TestMethodExecutionExtensions
    {
        public static Task<TResult> Exec<T, TResult>(this T instance, Expression<Func<T, TResult>> expression, bool useAsync)
        {
            return Exec_Internal<TResult>(expression, instance, useAsync);
        }

        public static Task Exec<T>(this T instance, Expression<Action<T>> expression, bool useAsync)
        {
            return Exec_Internal(expression, instance, useAsync);
        }

        public static Task<TResult> Exec<TResult>(this object o, Expression<Func<TResult>> expression, bool useAsync)
        {
            return Exec_Internal<TResult>(expression, null, useAsync);
        }

        public static Task Exec(this object o, Expression<Action> expression, bool useAsync)
        {
            return Exec_Internal(expression, null, useAsync);
        }

        private static Task<TResult> Exec_Internal<TResult>(LambdaExpression expression, object instance, bool useAsync)
        {
            var callExpression = (MethodCallExpression)expression.Body;
            MethodInfo method = callExpression.Method;
            object[] arguments = GetArguments(callExpression);

            try
            {
                if (useAsync)
                {
                    MethodInfo asyncMethod = GetAsyncMethod(method);
                    var task = (Task<TResult>)asyncMethod.Invoke(instance, arguments);
                    return task;
                }

                var result = (TResult)method.Invoke(instance, arguments);
                return Task.FromResult(result);
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
            }
        }

        private static Task Exec_Internal(LambdaExpression expression, object instance, bool useAsync)
        {
            var callExpression = (MethodCallExpression)expression.Body;
            MethodInfo method = callExpression.Method;
            object[] arguments = GetArguments(callExpression);

            try
            {
                if (useAsync)
                {
                    MethodInfo asyncMethod = GetAsyncMethod(method);
                    var task = (Task)asyncMethod.Invoke(instance, arguments);
                    return task;
                }

                method.Invoke(instance, arguments);
                return Task.FromResult(0);
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
            }
        }

        private static object[] GetArguments(MethodCallExpression callExpression)
        {
            return callExpression.Arguments.Select(GetValue).ToArray();
        }

        private static MethodInfo GetAsyncMethod(MethodInfo syncMethod)
        {
            Type[] parameterTypes = syncMethod.GetParameters().Select(p => p.ParameterType).ToArray();
            var asyncMethodName = syncMethod.Name + "Async";
            MethodInfo asyncMethod = syncMethod.DeclaringType.GetMethod(asyncMethodName, parameterTypes);
            if (asyncMethod == null)
            {
                throw new InvalidOperationException($"Method {syncMethod.DeclaringType.FullName}.{asyncMethodName} was not found.");
            }

            return asyncMethod;
        }

        private static object GetValue(Expression member)
        {
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }
    }
}