using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Xunit;
using Xunit.Extensions;

namespace NDbfReader.Tests
{
    public sealed class ReaderTests
    {
        private const string EXPECTED_NO_ROWS_EXCEPTION_MESSAGE = "No row is loaded. Call Read method first and check whether it returns true.";

        private const string ZERO_SIZE_COLUMN_NAME = "KRAJID";

        [Fact]
        public void Table_OpenedReader_ReturnsReferenceToTheParentTable()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                // Act
                var actualTable = reader.Table;

                // Assert
                Assert.Same(table, actualTable);
            }
        }

        [Fact]
        public void Table_DisposedTable_ThrowsObjectDisposedException()
        {
            PublicInterfaceInteraction_DisposedTable_ThrowsObjectDisposedException(reader => reader.Table);
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_DisposedTable_ThrowsObjectDisposedException(string methodName, Type parameterType)
        {
            PublicInterfaceInteraction_DisposedTable_ThrowsObjectDisposedException(
                reader => ExecuteGetMethod(reader, methodName, parameterType, GetValidArgument(reader, parameterType)));
        }

        [Theory]
        [ReaderGetMethods(exclude: "GetValue")]
        public void GetMethod_MismatchedColumnInstance_ThrowsArgumentOutOfRangeExeptionException(string methodName)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(
                    () => ExecuteGetMethod(reader, methodName, parameterType: typeof(IColumn), parameter: GetMismatchedColumnInstance(reader, methodName)));

                exception.ParamName.Should().BeEquivalentTo("column");
                exception.Message.Should().StartWithEquivalent("The column's type does not match the method's return type.");
            }
        }

        [Theory]
        [ReaderGetMethods(exclude: "GetValue")]
        public void GetMethod_MismatchedColumnName_ThrowsArgumentOutOfRangeExeptionException(string methodName)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(
                    () => ExecuteGetMethod(reader, methodName, parameterType: typeof(string), parameter: GetMismatchedColumnName(reader, methodName)));

                exception.ParamName.Should().BeEquivalentTo("columnName");
                exception.Message.Should().StartWithEquivalent("The column's type does not match the method's return type.");
            }
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_ColumnInstanceFromDifferentTable_ThrowsArgumentOutOfRangeExeptionException(string methodName)
        {
            // Arrange
            using (var differentTable = Samples.OpenBasicTable())
            using (var table = Samples.OpenBasicTable())
            {
                var differentColumn = differentTable.Columns.First();

                var reader = table.OpenReader();
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(
                    () => ExecuteGetMethod(reader, methodName, parameterType: typeof(IColumn), parameter: differentColumn));

                exception.ParamName.Should().BeEquivalentTo("column");
                exception.Message.Should().StartWithEquivalent("The column instance doesn't belong to this table.");
            }
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_NullColumnInstance_ThrowsArgumentNullExeptionException(string methodName)
        {
            GetMethod_NullParameter_ThrowsArgumentNullExeptionException(methodName, typeof(IColumn), "column");
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_NullColumnName_ThrowsArgumentNullExeptionException(string methodName)
        {
            GetMethod_NullParameter_ThrowsArgumentNullExeptionException(methodName, typeof(string), "columnName");
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_NonExistingColumnName_ThrowsArgumentOutOfRangeException(string methodName)
        {
            var nonExistingColumnName = "FOO";

            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(() => ExecuteGetMethod(reader, methodName, parameterType: typeof(string), parameter: nonExistingColumnName));
                exception.ParamName.Should().BeEquivalentTo("columnName");
                exception.Message.Should().StartWithEquivalent("Column " + nonExistingColumnName + " not found.");
            }
        }

        [Fact]
        public void TextEncoding_ReaderOpenedWithUnspecifiedEncoding_ReturnsTheASCIIEncoding()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                // Act
                var reader = table.OpenReader();

                // Assert 
                Assert.Same(Encoding.ASCII, reader.Encoding);
            }
        }

        [Fact]
        public void TextEncoding_ReaderOpenedWithEncoding_ReturnsTheSameEncoding()
        {
            // Arrange
            var utf8Encoding = Encoding.UTF8;

            using (var table = Samples.OpenBasicTable())
            {
                // Act
                var reader = table.OpenReader(utf8Encoding);

                // Assert 
                Assert.Same(utf8Encoding, reader.Encoding);
            }
        }

        [Fact]
        public void Read_TableBasedOnNonSeekableStream_ReadsAllRowsAsUsual()
        {
            // Arrange
            var streamSpy = Spy.OnStream(EmbeddedSamples.GetStream(EmbeddedSamples.BASIC));
            streamSpy.CanSeek.Returns(false);
            streamSpy.Seek(Arg.Any<long>(), Arg.Any<SeekOrigin>()).Returns(x => { throw new NotSupportedException(); });
            streamSpy.Position.Returns(x => { throw new NotSupportedException(); });
            streamSpy.When(s => s.Position = Arg.Any<long>()).Do(x => { throw new NotSupportedException(); });

            // Act
            int rowsCount = 0;

            using (var table = Table.Open(streamSpy))
            {
                var reader = table.OpenReader();

                while (reader.Read())
                    rowsCount += 1;
            }

            // Assert
            Assert.Equal(3, rowsCount);
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_ReadMethodNeverCalled_ThrowsInvalidOperationException(string methodName, Type parameterType)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                // Act & Assert
                var exception = Assert.Throws<InvalidOperationException>(() => ExecuteGetMethod(reader, methodName, parameterType, GetValidArgument(reader, parameterType)));
                Assert.Equal(EXPECTED_NO_ROWS_EXCEPTION_MESSAGE, exception.Message);
            }
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_ReadMethodReturnedFalse_ThrowsInvalidOperationException(string methodName, Type parameterType)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                while (reader.Read()) { }

                // Act & Assert
                var exception = Assert.Throws<InvalidOperationException>(() => ExecuteGetMethod(reader, methodName, parameterType, GetValidArgument(reader, parameterType)));

                Assert.Equal(EXPECTED_NO_ROWS_EXCEPTION_MESSAGE, exception.Message);
            }
        }


        [Theory]
        [InlineData("GetString", "TEXT")]
        [InlineData("GetValue", "TEXT")]
        [InlineData("GetDecimal", "NUMERIC")]
        [InlineData("GetValue", "NUMERIC")]
        [InlineData("GetBoolean", "LOGICAL")]
        [InlineData("GetValue", "LOGICAL")]
        [InlineData("GetDate", "DATE")]
        [InlineData("GetValue", "DATE")]
        [InlineData("GetInt32", "LONG")]
        [InlineData("GetValue", "LONG")]
        public void GetMethod_ColumnName_ReturnsValue(string methodName, string columnName)
        {
            // Arrange
            var actualValues = new List<object>();
            var expectedValues = Samples.BasicTableContent[columnName];

            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                // Act
                while (reader.Read())
                {
                    actualValues.Add(ExecuteGetMethod(reader, methodName, typeof(string), columnName));
                }
            }

            //Assert
            actualValues.ShouldAllBeEquivalentTo(expectedValues);
        }

        [Theory]
        [InlineData("GetString", "TEXT")]
        [InlineData("GetValue", "TEXT")]
        [InlineData("GetDecimal", "NUMERIC")]
        [InlineData("GetValue", "NUMERIC")]
        [InlineData("GetBoolean", "LOGICAL")]
        [InlineData("GetValue", "LOGICAL")]
        [InlineData("GetDate", "DATE")]
        [InlineData("GetValue", "DATE")]
        [InlineData("GetInt32", "LONG")]
        [InlineData("GetValue", "LONG")]
        public void GetMethod_ColumnInstance_ReturnsValue(string methodName, string columnName)
        {
            // Arrange
            var actualValues = new List<object>();
            var expectedValues = Samples.BasicTableContent[columnName];

            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();
                var column = table.Columns.Single(c => c.Name == columnName);

                // Act
                while (reader.Read())
                {
                    actualValues.Add(ExecuteGetMethod(reader, methodName, typeof(IColumn), column));
                }
            }

            //Assert
            actualValues.ShouldAllBeEquivalentTo(expectedValues);
        }

        [Fact]
        public void Read_TableWithDeletedRows_SkipsDeletedRows()
        {
            // Arrange
            using (var table = Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.DELETED_ROWS)))
            {
                var reader = table.OpenReader();
                var expectedItems = new List<string>() { "text3" };
                var actualItems = new List<string>();

                // Act
                while (reader.Read())
                {
                    actualItems.Add(reader.GetString("NAME"));
                }

                // Assert
                actualItems.ShouldAllBeEquivalentTo(expectedItems);
            }
        }

        [Fact]
        public void GetString_ReaderOfTableWithCzechTextsOpenedWithCzechEncoding_ReturnsCorrectlyEncodedString()
        {
            // Arrange
            using (var table = Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.CZECH_ENCODING)))
            {
                var reader = table.OpenReader(Encoding.GetEncoding(1250));

                var expectedItems = new List<string>() { "Mateřská škola Deštná", "Tělocvična Deštná", "Městský úřad Deštná" };
                var actualItems = new List<string>();

                // Act
                while (reader.Read())
                {
                    actualItems.Add(reader.GetString("DS"));
                }

                // Assert
                actualItems.ShouldAllBeEquivalentTo(expectedItems);
            }
        }

        [Fact]
        public void GetMethod_PreviousColumnNameOnNonSeekableStream_ThrowsInvalidOperationException()
        {
            GetMethod_OutOfOrderColumnOnNonSeekableStream_ThrowsInvalidOperationException(reader => reader.GetDate("DATE"), reader => reader.GetString("TEXT"));
        }

        [Fact]
        public void GetMethod_RepeatedColumnNameOnNonSeekableStream_ThrowsInvalidOperationException()
        {
            Action<Reader> getMethodCall = (reader) => reader.GetDate("DATE");

            GetMethod_OutOfOrderColumnOnNonSeekableStream_ThrowsInvalidOperationException(getMethodCall, getMethodCall);
        }

        [Fact]
        public void GetMethod_PreviousColumnInstanceOnNonSeekableStream_ThrowsInvalidOperationException()
        {
            GetMethod_OutOfOrderColumnOnNonSeekableStream_ThrowsInvalidOperationException(reader => reader.GetDate(reader.Table.Columns[1]), reader => reader.GetString(reader.Table.Columns[0]));
        }

        [Fact]
        public void GetMethod_RepeatedColumnInstanceOnNonSeekableStream_ThrowsInvalidOperationException()
        {
            Action<Reader> getMethodCall = (reader) => reader.GetDate(reader.Table.Columns[1]);

            GetMethod_OutOfOrderColumnOnNonSeekableStream_ThrowsInvalidOperationException(getMethodCall, getMethodCall);
        }

        [Fact]
        public void GetMethod_PreviousColumnInstanceOnSeekableStream_ReturnsCorrectValue()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                reader.Read();

                var dateColumn = table.Columns[1];
                var textColumn = table.Columns[0];

                // Act
                reader.GetDate(dateColumn);
                var actualText = reader.GetString(textColumn);

                // Assert
                var expectedText = Samples.BasicTableContent["TEXT"].First();
                Assert.Equal(expectedText, actualText);
            }
        }

        [Fact]
        public void GetMethod_PreviousColumnNameOnSeekableStream_ReturnsCorrectValue()
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                reader.Read();

                // Act
                reader.GetDate("DATE");
                var actualText = reader.GetString("TEXT");

                // Assert
                var expectedText = Samples.BasicTableContent["TEXT"].First();
                Assert.Equal(expectedText, actualText);
            }
        }

        [Fact]
        public void GetMethod_RepeatedColumnInstanceOnSeeeakbleStream_ReturnsTheSameValue()
        {
            GetMethod_RepeatedColumnOnSeekableStream_ReturnsTheSameValue(reader => reader.GetDate(reader.Table.Columns[1]));
        }

        [Fact]
        public void GetMethod_RepeatedColumnNameOnSeeeakbleStream_ReturnsTheSameValue()
        {
            GetMethod_RepeatedColumnOnSeekableStream_ReturnsTheSameValue(reader => reader.GetDate("DATE"));
        }

        [Fact]
        public void GetDecimal_ZeroSizeColumnName_ReturnsNull()
        {
            GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetDecimal(ZERO_SIZE_COLUMN_NAME));
        }

        [Fact]
        public void GetValue_ZeroSizeColumnName_ReturnsNull()
        {
            GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetValue(ZERO_SIZE_COLUMN_NAME));
        }

        [Fact]
        public void GetDecimal_ZeroSizeColumnInstance_ReturnsNull()
        {
            GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetDecimal(GetZeroSizeColumn(reader.Table)));
        }

        [Fact]
        public void GetValue_ZeroSizeColumnInstance_ReturnsNull()
        {
            GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetValue(GetZeroSizeColumn(reader.Table)));
        }

        private void GetMethod_ZeroSizeColumn_ReturnsNull(Func<Reader, object> getter)
        {
            // Arrange
            using (var table = Table.Open(EmbeddedSamples.GetStream(EmbeddedSamples.ZERO_SIZE_COLUMN)))
            {
                var reader = table.OpenReader(Encoding.GetEncoding(1250));
                reader.Read();

                var expectedValues = new[] { null, "ABCD"};

                // Act
                var actualValues = new[] { getter(reader), reader.GetString("CSU_KRAJ") };

                // Assert
                actualValues.ShouldAllBeEquivalentTo(expectedValues);
            }
        }

        private static IColumn GetMismatchedColumnInstance(Reader reader, string methodName)
        {
            var methodInfo = reader.GetType().GetMethod(methodName, new[] { typeof(IColumn) });
            var returnType = Nullable.GetUnderlyingType(methodInfo.ReturnType) ?? methodInfo.ReturnType;

            return reader.Table.Columns.First(column => column.Type != returnType);
        }

        private static string GetMismatchedColumnName(Reader reader, string methodName)
        {
            var methodInfo = reader.GetType().GetMethod(methodName, new[] { typeof(string) });
            var returnType = Nullable.GetUnderlyingType(methodInfo.ReturnType) ?? methodInfo.ReturnType;

            return reader.Table.Columns.First(column => column.Type != returnType).Name;
        }

        private void GetMethod_NullParameter_ThrowsArgumentNullExeptionException(string methodName, Type paramaterType, string parameterName)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentNullException>(
                    () => ExecuteGetMethod(reader, methodName, parameterType: paramaterType, parameter: null));

                Assert.Equal(parameterName, exception.ParamName);
            }
        }

        private void GetMethod_RepeatedColumnOnSeekableStream_ReturnsTheSameValue(Func<Reader, object> getCall)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader();

                reader.Read();
                
                // Act
                var firstValue = getCall(reader);
                var secondValue = getCall(reader);

                // Assert
                Assert.Equal(firstValue, secondValue);
            }
        }

        private void PublicInterfaceInteraction_DisposedTable_ThrowsObjectDisposedException(Func<Reader, object> action)
        {
            // Arrange
            var table = Samples.OpenBasicTable();
            var reader = table.OpenReader();
            table.Dispose();

            // Act & Assert
            var exception = Assert.Throws<ObjectDisposedException>(() => action(reader));
            Assert.Equal(typeof(Table).FullName, exception.ObjectName);
        }

        private void GetMethod_OutOfOrderColumnOnNonSeekableStream_ThrowsInvalidOperationException(Action<Reader> firstGetCall, Action<Reader> secondGetCall)
        {
            // Arrange
            var nonSeeekableStream = Spy.OnStream(EmbeddedSamples.GetStream(EmbeddedSamples.BASIC));
            nonSeeekableStream.CanSeek.Returns(false);

            using (var table = Table.Open(nonSeeekableStream))
            {
                var reader = table.OpenReader();

                reader.Read();
                firstGetCall(reader);

                // Act & Assert
                var exception = Assert.Throws<InvalidOperationException>(() => secondGetCall(reader));
                Assert.Equal("The underlying non-seekable stream does not allow reading the columns out of order.", exception.Message);
            }
        }

        private static object GetValidArgument(Reader reader, Type type)
        {
            var firstColumn = reader.Table.Columns.First();

            if (type == typeof(string))
            {
                return firstColumn.Name;
            }
            if (type == typeof(IColumn))
            {
                return firstColumn;
            }

            throw new ArgumentOutOfRangeException("type");
        }

        private static object ExecuteGetMethod(Reader reader, string methodName, Type parameterType, object parameter = null)
        {
            var methodInfo = typeof(Reader).GetMethod(methodName, new[] { parameterType });
            if (methodInfo == null) throw new ArgumentOutOfRangeException("methodName", "Method " + methodName + " not found.");

            try
            {
                return methodInfo.Invoke(reader, new object[] { parameter });
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
            }
        }

        private static IColumn GetZeroSizeColumn(Table table)
        {
            return table.Columns.Single(c => c.Name == ZERO_SIZE_COLUMN_NAME);
        }
    }
}
