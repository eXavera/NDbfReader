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

Clone the repository and run `build.cmd`. Opening the solution requires Visual Studio 2010 or newer (including Express editions).

## Tests

Run `run-tests.cmd`.

Run `test-coverage.cmd` to calculate test coverage with [OpenCover](https://github.com/OpenCover/opencover).

## License
[MIT](https://github.com/eXavera/NDbfReader/blob/master/LICENSE.md)
