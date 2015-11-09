using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using NDbfReader.Tests.Infrastructure;
using NSubstitute;
using Xunit;

namespace NDbfReader.Tests
{
    public sealed class ReaderTests
    {
        private const string EXPECTED_NO_ROWS_EXCEPTION_MESSAGE = "No row is loaded. Call Read method first and check whether it returns true.";

        private const string ZERO_SIZE_COLUMN_NAME = "KRAJID";

        [Theory]
        [InlineDataWithExecMode]
        public Task GetDecimal_ZeroSizeColumnInstance_ReturnsNull(bool useAsync)
        {
            return GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetDecimal(GetZeroSizeColumn(reader.Table)), useAsync);
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task GetDecimal_ZeroSizeColumnName_ReturnsNull(bool useAsync)
        {
            return GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetDecimal(ZERO_SIZE_COLUMN_NAME), useAsync);
        }

        [Theory]
        [InlineDataWithExecMode("GetString", "TEXT")]
        [InlineDataWithExecMode("GetValue", "TEXT")]
        [InlineDataWithExecMode("GetDecimal", "NUMERIC")]
        [InlineDataWithExecMode("GetValue", "NUMERIC")]
        [InlineDataWithExecMode("GetBoolean", "LOGICAL")]
        [InlineDataWithExecMode("GetValue", "LOGICAL")]
        [InlineDataWithExecMode("GetDate", "DATE")]
        [InlineDataWithExecMode("GetValue", "DATE")]
        [InlineDataWithExecMode("GetInt32", "LONG")]
        [InlineDataWithExecMode("GetValue", "LONG")]
        public async Task GetMethod_ColumnInstance_ReturnsValue(bool useAsync, string methodName, string columnName)
        {
            // Arrange
            var actualValues = new List<object>();
            var expectedValues = Samples.BasicTableContent[columnName];

            using (var table = await OpenBasicTable(useAsync))
            {
                var reader = table.OpenReader();
                var column = table.Columns.Single(c => c.Name == columnName);

                // Act
                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    actualValues.Add(ExecuteGetMethod(reader, methodName, typeof(IColumn), column));
                }
            }

            //Assert
            actualValues.ShouldAllBeEquivalentTo(expectedValues);
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_ColumnInstanceFromDifferentTable_ThrowsArgumentOutOfRangeExeptionException(string methodName)
        {
            // Arrange
            Type columnType = typeof(Reader).GetMethod(methodName, new[] { typeof(IColumn) }).ReturnType;

            using (var differentTable = Samples.OpenBasicTable())
            using (var table = Samples.OpenBasicTable())
            {
                IColumn differentColumn = null;
                if (columnType == typeof(object))
                {
                    differentColumn = differentTable.Columns.First();
                }
                else
                {
                    differentColumn = differentTable.Columns.Single(c => c.Type == columnType);
                }

                var reader = table.OpenReader();
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(
                    () => ExecuteGetMethod(reader, methodName, parameterType: typeof(IColumn), parameter: differentColumn));

                exception.ParamName.Should().BeEquivalentTo("column");
                exception.Message.Should().StartWithEquivalent("The column instance not found.");
            }
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_ColumnInstanceUnspecifiedInOpenReader_ThrowsArgumentOutOfRangeException(string getMethodName)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                string invalidColumnName = table.Columns[1].Name;
                var reader = table.OpenReader(new[] { table.Columns.First() });
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(
                    () => ExecuteGetMethod(reader, getMethodName, parameterType: typeof(string), parameter: invalidColumnName));
                exception.ParamName.Should().BeEquivalentTo("columnName");
                exception.Message.Should().StartWithEquivalent($"Column {invalidColumnName} not found.");
            }
        }

        [Theory]
        [InlineDataWithExecMode("GetString", "TEXT")]
        [InlineDataWithExecMode("GetValue", "TEXT")]
        [InlineDataWithExecMode("GetDecimal", "NUMERIC")]
        [InlineDataWithExecMode("GetValue", "NUMERIC")]
        [InlineDataWithExecMode("GetBoolean", "LOGICAL")]
        [InlineDataWithExecMode("GetValue", "LOGICAL")]
        [InlineDataWithExecMode("GetDate", "DATE")]
        [InlineDataWithExecMode("GetValue", "DATE")]
        [InlineDataWithExecMode("GetInt32", "LONG")]
        [InlineDataWithExecMode("GetValue", "LONG")]
        public async Task GetMethod_ColumnName_ReturnsValue(bool useAsync, string methodName, string columnName)
        {
            // Arrange
            var actualValues = new List<object>();
            var expectedValues = Samples.BasicTableContent[columnName];

            using (var table = await OpenBasicTable(useAsync))
            {
                var reader = table.OpenReader();

                // Act
                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    actualValues.Add(ExecuteGetMethod(reader, methodName, typeof(string), columnName));
                }
            }

            //Assert
            actualValues.ShouldAllBeEquivalentTo(expectedValues);
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_ColumnNameUnspecifiedInOpenReader_ThrowsArgumentOutOfRangeException(string getMethodName)
        {
            GetMethod_InvalidColumnName_ThrowsArgumentOutOfRangeException("LONG", new[] { "TEXT", "DATE" }, getMethodName);
        }

        [Theory]
        [ReaderGetMethods]
        public void GetMethod_DisposedTable_ThrowsObjectDisposedException(string methodName, Type parameterType)
        {
            PublicInterfaceInteraction_DisposedTable_ThrowsObjectDisposedException(
                reader => ExecuteGetMethod(reader, methodName, parameterType, GetValidArgument(reader, methodName, parameterType)));
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
        public void GetMethod_NonExistingColumnName_ThrowsArgumentOutOfRangeException(string getMethodName)
        {
            GetMethod_InvalidColumnName_ThrowsArgumentOutOfRangeException("FOO", new string[] { }, getMethodName);
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
        [InlineDataWithExecMode("TEXT,NUMERIC")] // side by side
        [InlineDataWithExecMode("NUMERIC,TEXT")] // out of order
        [InlineDataWithExecMode("TEXT,DATE")] // holes in between
        [InlineDataWithExecMode("NUMERIC,DATE")] // hole at the beggining
        [InlineDataWithExecMode("LOGICAL")] // single
        public Task GetMethod_OpenReadWithExplicitColumnInstances_ReturnsValuesOfGivenColumns(bool useAsync, string columnsToLoad)
        {
            return GetMethod_OpenReaderWithExplicitColumns_ReturnsValuesOfGivenColumns(useAsync, (table, columnNames) =>
            {
                List<IColumn> columns = columnNames.Select(name => table.Columns.Single(c => c.Name == name)).ToList();
                return table.OpenReader(columns);
            }, columnsToLoad);
        }

        [Theory]
        [InlineDataWithExecMode("TEXT,NUMERIC")] // side by side
        [InlineDataWithExecMode("NUMERIC,TEXT")] // out of order
        [InlineDataWithExecMode("TEXT,DATE")] // holes in between
        [InlineDataWithExecMode("NUMERIC,DATE")] // hole at the beggining
        [InlineDataWithExecMode("LOGICAL")] // single
        public Task GetMethod_OpenReadWithExplicitColumnNames_ReturnsValuesOfGivenColumns(bool useAsync, string columnsToLoad)
        {
            return GetMethod_OpenReaderWithExplicitColumns_ReturnsValuesOfGivenColumns(useAsync, (table, columnNames) => table.OpenReader(columnNames), columnsToLoad);
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task GetMethod_PreviousColumnInstanceOnSeekableStream_ReturnsCorrectValue(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var reader = table.OpenReader();

                await reader.Exec(r => r.Read(), useAsync);

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

        [Theory]
        [InlineDataWithExecMode]
        public async Task GetMethod_PreviousColumnNameOnSeekableStream_ReturnsCorrectValue(bool useAsync)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var reader = table.OpenReader();

                await reader.Exec(r => r.Read(), useAsync);

                // Act
                reader.GetDate("DATE");
                var actualText = reader.GetString("TEXT");

                // Assert
                var expectedText = Samples.BasicTableContent["TEXT"].First();
                Assert.Equal(expectedText, actualText);
            }
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
                var exception = Assert.Throws<InvalidOperationException>(() => ExecuteGetMethod(reader, methodName, parameterType, GetValidArgument(reader, methodName, parameterType)));
                Assert.Equal(EXPECTED_NO_ROWS_EXCEPTION_MESSAGE, exception.Message);
            }
        }

        [Theory]
        [ReaderGetMethods]
        public async Task GetMethod_ReadMethodReturnedFalse_ThrowsInvalidOperationException(bool useAsync, string methodName, Type parameterType)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var reader = table.OpenReader();

                while (await reader.Exec(r => r.Read(), useAsync)) { }

                // Act & Assert
                var exception = Assert.Throws<InvalidOperationException>(() => ExecuteGetMethod(reader, methodName, parameterType, GetValidArgument(reader, methodName, parameterType)));

                Assert.Equal(EXPECTED_NO_ROWS_EXCEPTION_MESSAGE, exception.Message);
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task GetMethod_RepeatedColumnInstanceOnSeeeakbleStream_ReturnsTheSameValue(bool useAsync)
        {
            return GetMethod_RepeatedColumnOnSeekableStream_ReturnsTheSameValue(useAsync, reader => reader.GetDate(reader.Table.Columns[1]));
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task GetMethod_RepeatedColumnNameOnSeeeakbleStream_ReturnsTheSameValue(bool useAsync)
        {
            return GetMethod_RepeatedColumnOnSeekableStream_ReturnsTheSameValue(useAsync, reader => reader.GetDate("DATE"));
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task GetString_ReaderOfTableWithCzechTextsOpenedWithCzechEncoding_ReturnsCorrectlyEncodedString(bool useAsync)
        {
            // Arrange
            Stream stream = EmbeddedSamples.GetStream(EmbeddedSamples.CZECH_ENCODING);
            using (var table = await this.Exec(() => Table.Open(stream), useAsync))
            {
                var reader = table.OpenReader(Encoding.GetEncoding(1250));

                var expectedItems = new List<string> { "Mateřská škola Deštná", "Tělocvična Deštná", "Městský úřad Deštná" };
                var actualItems = new List<string>();

                // Act
                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    actualItems.Add(reader.GetString("DS"));
                }

                // Assert
                actualItems.ShouldAllBeEquivalentTo(expectedItems);
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task GetValue_ZeroSizeColumnInstance_ReturnsNull(bool useAsync)
        {
            return GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetValue(GetZeroSizeColumn(reader.Table)), useAsync);
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task GetValue_ZeroSizeColumnName_ReturnsNull(bool useAsync)
        {
            return GetMethod_ZeroSizeColumn_ReturnsNull(reader => reader.GetValue(ZERO_SIZE_COLUMN_NAME), useAsync);
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Read_RepeatedCall_SkipsRows(bool useAsync)
        {
            object[] expectedSecondRowContent = Samples.BasicTableContent.Select(pair => pair.Value[1]).ToArray();

            using (Table table = await OpenBasicTable(useAsync))
            {
                Reader reader = table.OpenReader();
                await reader.Exec(r => r.Read(), useAsync);
                await reader.Exec(r => r.Read(), useAsync);

                object[] secondRowContent = table.Columns.Select(reader.GetValue).ToArray();

                secondRowContent.ShouldAllBeEquivalentTo(expectedSecondRowContent);
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Read_TableBasedOnNonSeekableStream_ReadsAllRowsAsUsual(bool useAsync)
        {
            // Arrange
            var streamSpy = MakeNonSeekanle((EmbeddedSamples.GetStream(EmbeddedSamples.BASIC)));

            // Act
            int rowsCount = 0;
            using (var table = await this.Exec(() => Table.Open(streamSpy), useAsync))
            {
                var reader = table.OpenReader();

                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    rowsCount += 1;
                }
            }

            // Assert
            Assert.Equal(3, rowsCount);
        }

        [Theory]
        [InlineDataWithExecMode]
        public async Task Read_TableOnNonSeekableStream_SkipsColumns(bool useAsync)
        {
            // Arrange
            Stream stream = MakeNonSeekanle(Samples.GetBasicTableStream());
            using (var table = await this.Exec(() => Table.Open(stream), useAsync))
            {
                Reader reader = table.OpenReader("TEXT", "LOGICAL");

                // Act
                await reader.Exec(r => r.Read(), useAsync);
                object[] result = { reader.GetValue("TEXT"), reader.GetValue("LOGICAL") };

                // Assert
                result.ShouldAllBeEquivalentTo(new object[] { Samples.BasicTableContent["TEXT"][0], Samples.BasicTableContent["LOGICAL"][0] });
            }
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task Read_TableOnNonSeekableStreamWithDeletedRows_SkipsDeletedRows(bool useAsync)
        {
            return Read_TableWithDeletedRows_SkipsDeletedRows(useAsync, MakeNonSeekanle);
        }

        [Theory]
        [InlineDataWithExecMode]
        public Task Read_TableWithDeletedRows_SkipsDeletedRows(bool useAsync)
        {
            return Read_TableWithDeletedRows_SkipsDeletedRows(useAsync, stream => stream);
        }

        [Fact]
        public void Table_DisposedTable_ThrowsObjectDisposedException()
        {
            PublicInterfaceInteraction_DisposedTable_ThrowsObjectDisposedException(reader => reader.Table);
        }

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

        private static object ExecuteGetMethod(Reader reader, string methodName, Type parameterType, object parameter = null)
        {
            var methodInfo = typeof(Reader).GetMethod(methodName, new[] { parameterType });
            if (methodInfo == null) throw new ArgumentOutOfRangeException(nameof(methodName), "Method " + methodName + " not found.");

            try
            {
                return methodInfo.Invoke(reader, new object[] { parameter });
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
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

        private static object GetValidArgument(Reader reader, string methodName, Type argType)
        {
            Type columnType = typeof(Reader).GetMethod(methodName, new[] { argType }).ReturnType;
            IColumn column = null;
            if (columnType == typeof(object))
            {
                column = reader.Table.Columns.First();
            }
            else
            {
                column = reader.Table.Columns.Single(c => c.Type == columnType);
            }

            if (argType == typeof(string))
            {
                return column.Name;
            }
            if (argType == typeof(IColumn))
            {
                return column;
            }

            throw new ArgumentOutOfRangeException(nameof(argType));
        }

        private static IColumn GetZeroSizeColumn(Table table)
        {
            return table.Columns.Single(c => c.Name == ZERO_SIZE_COLUMN_NAME);
        }

        private static Stream MakeNonSeekanle(Stream stream)
        {
            var streamSpy = Spy.OnStream(stream);
            streamSpy.CanSeek.Returns(false);
            streamSpy.Seek(Arg.Any<long>(), Arg.Any<SeekOrigin>()).Returns(x => { throw new NotSupportedException(); });
            streamSpy.Position.Returns(x => { throw new NotSupportedException(); });
            streamSpy.When(s => s.Position = Arg.Any<long>()).Do(x => { throw new NotSupportedException(); });

            return streamSpy;
        }

        private void GetMethod_InvalidColumnName_ThrowsArgumentOutOfRangeException(string invalidColumnName, string[] explicitColumnNames, string getMethodName)
        {
            // Arrange
            using (var table = Samples.OpenBasicTable())
            {
                var reader = table.OpenReader(explicitColumnNames);
                reader.Read();

                // Act & Assert
                var exception = Assert.Throws<ArgumentOutOfRangeException>(
                    () => ExecuteGetMethod(reader, getMethodName, parameterType: typeof(string), parameter: invalidColumnName));
                exception.ParamName.Should().BeEquivalentTo("columnName");
                exception.Message.Should().StartWithEquivalent($"Column {invalidColumnName} not found.");
            }
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

        private async Task GetMethod_OpenReaderWithExplicitColumns_ReturnsValuesOfGivenColumns(bool useAsync, Func<Table, string[], Reader> readerOpener, string columnNamesToLoad)
        {
            string[] columnNames = columnNamesToLoad.Split(',');
            object[] expectedValues = columnNames.Select(columnName => Samples.BasicTableContent[columnName][0]).ToArray();

            using (Table table = await OpenBasicTable(useAsync))
            {
                Reader reader = readerOpener(table, columnNames);
                await reader.Exec(r => r.Read(), useAsync);

                object[] actualValues = columnNames.Select(reader.GetValue).ToArray();

                actualValues.ShouldBeEquivalentTo(expectedValues);
            }
        }

        private async Task GetMethod_RepeatedColumnOnSeekableStream_ReturnsTheSameValue(bool useAsync, Func<Reader, object> getCall)
        {
            // Arrange
            using (var table = await OpenBasicTable(useAsync))
            {
                var reader = table.OpenReader();

                await reader.Exec(r => r.Read(), useAsync);

                // Act
                var firstValue = getCall(reader);
                var secondValue = getCall(reader);

                // Assert
                Assert.Equal(firstValue, secondValue);
            }
        }

        private async Task GetMethod_ZeroSizeColumn_ReturnsNull(Func<Reader, object> getter, bool useAsync)
        {
            // Arrange
            Stream stream = EmbeddedSamples.GetStream(EmbeddedSamples.ZERO_SIZE_COLUMN);
            using (var table = await this.Exec(() => Table.Open(stream), useAsync))
            {
                var reader = table.OpenReader(Encoding.GetEncoding(1250));
                await reader.Exec((r) => r.Read(), useAsync);

                var expectedValues = new[] { null, "ABCD" };

                // Act
                var actualValues = new[] { getter(reader), reader.GetString("CSU_KRAJ") };

                // Assert
                actualValues.ShouldAllBeEquivalentTo(expectedValues);
            }
        }

        private Task<Table> OpenBasicTable(bool useAsync)
        {
            return this.Exec(() => Samples.OpenBasicTable(), useAsync);
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

        private async Task Read_TableWithDeletedRows_SkipsDeletedRows(bool useAsync, Func<Stream, Stream> streamModifier)
        {
            // Arrange
            var stream = streamModifier(EmbeddedSamples.GetStream(EmbeddedSamples.DELETED_ROWS));
            using (var table = await this.Exec(() => Table.Open(stream), useAsync))
            {
                var reader = table.OpenReader();
                var expectedItems = new List<string> { "text3" };
                var actualItems = new List<string>();

                // Act
                while (await reader.Exec(r => r.Read(), useAsync))
                {
                    actualItems.Add(reader.GetString("NAME"));
                }

                // Assert
                actualItems.ShouldAllBeEquivalentTo(expectedItems);
            }
        }
    }
}