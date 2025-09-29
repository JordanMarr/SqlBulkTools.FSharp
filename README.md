<img src="http://gregnz.com/images/SqlBulkTools/icon-large.png" alt="SqlBulkTools">  

* SqlBulkTools.FSharp is a wrapper around SqlBulkTools which provides F# computation expressions: 
  * `bulkInsert`
  * `bulkUpdate`
  * `bulkUpsert`
  * `bulkDelete`

* SqlBulkTools.FSharp v1.0.0 currently relies on a fork of SqlBulkTools by [fretje](https://github.com/fretje/SqlBulkTools) which migrated to `Microsoft.Data.SqlClient`.
  * (Previous versions used `System.Data.SqlClient`.)
  * If you need to use System.Data.SqlClient, there is also a fork by fretje called [SqlBulkTools.SystemDataSqlClient](https://www.nuget.org/packages/fretje.SqlBulkTools.SystemDataSqlClient)

## Installation
[![NuGet version (SqlBulkTools.FSharp)](https://img.shields.io/nuget/v/SqlBulkTools.FSharp.svg?style=flat-square)](https://www.nuget.org/packages/SqlBulkTools.FSharp/)

```dotnet add package SqlBulkTools.FSharp```


## F# Computation Expressions!

```open SqlBulkTools.FSharp``` 
```open Microsoft.Data.SqlClient``` 

### Bulk Insert
```fsharp
use conn = new new SqlConnection("conn str..")

let count =
    bulkInsert conn {
        for user in users do
        table "Users"
        column user.Id
        columnMap user.FirstName "FName"
        columnMap user.LastName "LName"
        column user.SSN
	identity user.Id
    } 
if count > 0 then tx.Commit() else tx.Rollback()
```

### Bulk Update
```fsharp
use conn = new new SqlConnection("conn str..")
// Explicit transaction is optional
use tx = conn.BeginTransaction()

bulkUpdate conn {
    for row in rows do
    transaction tx
    table (nameof ctx.Dbo.Orders)
    column row.Id
    column row.OrderDate
    column row.CustomerEmail
    column row.CustomerAddress
    column row.CustomerCity
    column row.CustomerState
    column row.CustomerZip
    matchTargetOn row.Id
}
```

### Bulk Upsert
```fsharp
bulkUpsert conn {
    for row in rows do
    table (nameof ctx.Dbo.Orders)
    column row.Id
    column row.OrderDate
    column row.CustomerEmail
    column row.CustomerAddress
    column row.CustomerCity
    column row.CustomerState
    column row.CustomerZip
    
    // to match against multiple columns:
    matchTargetOn row.OrderDate 
    matchTargetOn row.Email
}
```

### Bulk Delete
```fsharp
bulkDelete conn {
    for sheet in deletedSheets do
    table "Sheets"
    column sheet.Id
    matchTargetOn sheet.Id
}
```

## Original Project Readme
You can use the underlying C# fluent API if the F# computation expression builders don't meet your needs:

Please view the original C# README.md for full documentation:

* [fretje Fork Readme](https://github.com/fretje/SqlBulkTools)
* [Original Project Readme](https://github.com/olegil/SqlBulkTools)
