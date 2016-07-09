NDbfReader
============

*Fully managed* .NET library for reading dBASE (.dbf) files.

- Fast and lightweight
- Full *async support*

Supported platforms:

- .NET 4.0 +
- .NET Core (.NET Standard 1.3)

## Example

```csharp
using (var table = Table.Open("D:\\foo.dbf"))
{
   // default is UTF-8 encoding
   var reader = table.OpenReader(Encoding.GetEncoding(1250));
   while(reader.Read())
   {
     var name = reader.GetString("NAME");
     //...
   }
}
```
An entire table can be loaded into a `DataTable`:
```
using (var table = Table.Open("D:\\foo.dbf"))
   return table.AsDataTable();
```
Non-seekable (forward-only) streams are also supported:
```csharp
[HttpPost]
public ActionResult Upload(HttpPostedFileBase file)
{
   using (var table = Table.Open(file.InputStream))
   //..
}
```

## NuGet

```
Install-Package NDbfReader
```

## Source

Install `Visual Studio 2015` and [.NET Core for Visual Studio](https://www.microsoft.com/net/core).

Clone the repository and run `build.cmd`.

## Tests

Run `run-tests.cmd`.

Run `test-coverage.cmd` to calculate test coverage with [OpenCover](https://github.com/OpenCover/opencover).

## License
[MIT](https://github.com/eXavera/NDbfReader/blob/master/LICENSE.md)
