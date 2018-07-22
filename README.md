NDbfReader
============

A *fully managed* .NET library for reading dBASE (.dbf) files.

- Fast and lightweight
- Full *async support*

Supported platforms:

- .NET 4.0 +
- .NET Standard 1.3 (without `AsDataTable` methods)
- .NET Standard 2.0

[Supported data types](https://github.com/eXavera/NDbfReader/wiki/Supported-data-types)

## Example

```csharp
using (var table = Table.Open(@"D:\mytable.dbf"))
{
    var reader = table.OpenReader(Encoding.ASCII);
    while (reader.Read())
    {
        var row = new MyRow()
        {
            Text = reader.GetString("TEXT"),
            DateTime = reader.GetDateTime("DATETIME"),
            IntValue = reader.GetInt32("INT"),
            DecimalValue = reader.GetDecimal("DECIMAL"),
            BooleanValue = reader.GetBoolean("BOOL")
        };
    }
}
```

## Installation

[![NuGet](https://img.shields.io/nuget/v/NDbfReader.svg)](https://www.nuget.org/packages/NDbfReader)

## Source

Install `Visual Studio 2017 (15.5+)` and .NET Core 2.0.

Clone the repository and run `build.cmd` from `Developer Command Prompt for VS`.

## Tests

Run the `run-tests.cmd` batch file.

Run the `test-coverage.cmd` batch file to calculate the test coverage with [OpenCover](https://github.com/OpenCover/opencover).

## License
[MIT](https://github.com/eXavera/NDbfReader/blob/master/LICENSE.md)
