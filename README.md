NDbfReader
============

NDbfReader is a .NET library for reading dBASE (.dbf) files. The library is simple, extensible and without any external dependencies.

## Example

```csharp
using (var table = Table.Open("D:\\foo.dbf"))
{
   // default is ASCII encoding
   var reader = table.OpenReader(Encoding.GetEncoding(1250));
   while(reader.Read())
   {
     var name = reader.GetString("NAME");
     //...
   }
}
```
A table can be opened even from a non-seekable (forward-only) stream:
```csharp
[HttpPost]
public ActionResult Upload(HttpPostedFileBase file)
{
   using (var table = Table.Open(file.InputStream))
   {
      //..
   }
}
```
Supports the following dBASE column types:

- Character as `String`
- Date as `DateTime`
- Long as `Int32`
- Logical as `Boolean`
- Numeric, Float as `Decimal`

## NuGet

```
Install-Package NDbfReader
```

## Source

Clone the repository and run `build.cmd`. Openning the solution requires Visual Studio 2010 or newer (including Express editions).

## Tests

Run `run-tests.cmd`.

Run `test-coverage.cmd` to calculate test coverage with [OpenCover](https://github.com/OpenCover/opencover).

## License
[MIT](https://github.com/eXavera/NDbfReader/blob/master/LICENSE.md)