module SqlBulkTools.FSharp

open System
open System.Linq.Expressions
open System.Data
open SqlBulkTools
open SqlBulkTools.BulkCopy
open Util

type Context<'T> = 
    {
        Transaction: IDbTransaction option
        Operation: Operation<'T>
    }

and Operation<'T> = 
    | OpNone
    //| OpSetup of Setup
    | OpForCollection of BulkForCollection<'T>
    | OpWithTable of BulkTable<'T>
    | OpAddColumn of BulkAddColumn<'T>
    | OpInsert of BulkInsert<'T>
    | OpUpdate of BulkUpdate<'T>
    | OpUpsert of BulkInsertOrUpdate<'T>
    | OpDelete of BulkDelete<'T>

[<AbstractClass>]
type BulkInsertBase(conn: IDbConnection) =
    let def = { Operation = OpNone; Transaction = None }

    member this.For (rows: seq<'T>, f: 'T -> Context<'T>) =
        { def with Operation = OpForCollection (BulkOperations().Setup().ForCollection(rows)) }

    member this.Yield _ =
        def

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, tx: IDbTransaction) =
        { ctx with Transaction = Some tx }

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, txMaybe: IDbTransaction option) =
        { ctx with Transaction = txMaybe }

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (ctx: Context<'T>, tbl) =
        match ctx.Operation with
        | OpForCollection bulk -> { ctx with Operation = OpWithTable(bulk.WithTable tbl) }
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnMap", MaintainsVariableSpace=true)>]
    member this.ColumnMap (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>, destination: string) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("identity", MaintainsVariableSpace=true)>]
    member this.IdentityColumn (ctx: Context<'T>, [<ProjectionParameter>] colExpr) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpInsert (bulk.BulkInsert().SetIdentityColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."


type BulkInsert(conn: IDbConnection) =
    inherit BulkInsertBase(conn)

    member this.Run ctx =
        match ctx.Operation with
        | OpAddColumn bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.BulkInsert().Commit(conn, tx)
            | None -> bulk.BulkInsert().Commit(conn)
        | OpInsert bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.Commit(conn, tx)
            | None -> bulk.Commit(conn)
        | _ ->
            failwith "Must add at least one column first."

type BulkInsertTask(conn: IDbConnection) =
    inherit BulkInsertBase(conn)

    member this.Run ctx =
        match ctx.Operation with
        | OpAddColumn bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.BulkInsert().CommitAsync(conn, tx)
            | None -> bulk.BulkInsert().CommitAsync(conn)
        | OpInsert bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx)
            | None -> bulk.CommitAsync(conn)
        | _ ->
            failwith "Must add at least one column first."

type BulkInsertAsync(conn: IDbConnection) =
    inherit BulkInsertBase(conn)

    member this.Run ctx =
        match ctx.Operation with
        | OpAddColumn bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.BulkInsert().CommitAsync(conn, tx) |> Async.AwaitTask
            | None -> bulk.BulkInsert().CommitAsync(conn) |> Async.AwaitTask
        | OpInsert bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx) |> Async.AwaitTask
            | None -> bulk.CommitAsync(conn) |> Async.AwaitTask
        | _ ->
            failwith "Must add at least one column first."

/// A bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected.
/// Notes: Only the columns configured (via AddColumn) will be evaluated.
let bulkInsert conn = BulkInsert(conn)

/// A task-based bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected.
/// Notes: Only the columns configured (via AddColumn) will be evaluated.
let bulkInsertTask conn = BulkInsertTask(conn)

/// An async bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected.
/// Notes: Only the columns configured (via AddColumn) will be evaluated.
let bulkInsertAsync conn = BulkInsertAsync(conn)


[<AbstractClass>]
type BulkUpdateBase(conn: IDbConnection) =
    let def = { Operation = OpNone; Transaction = None }

    member this.For (rows: seq<'T>, f: 'T -> Context<'T>) =
        { def with Operation = OpForCollection (BulkOperations().Setup().ForCollection(rows)) }

    member this.Yield _ =
        def

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, tx: IDbTransaction) =
        { ctx with Transaction = Some tx }

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, txMaybe: IDbTransaction option) =
        { ctx with Transaction = txMaybe }

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (ctx: Context<'T>, tbl) =
        match ctx.Operation with
        | OpForCollection bulk -> { ctx with Operation = OpWithTable(bulk.WithTable tbl) }
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnMap", MaintainsVariableSpace=true)>]
    member this.ColumnMap<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>, destination: string) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("matchTargetOn", MaintainsVariableSpace=true)>]
    member this.MatchTargetOn<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpUpdate (bulk.BulkUpdate().MatchTargetOn(getPropertyName colExpr)) }
        | OpUpdate bulk -> { ctx with Operation = OpUpdate (bulk.MatchTargetOn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."

    [<CustomOperation("updateWhen", MaintainsVariableSpace=true)>]
    member this.UpdateWhen (ctx: Context<'T>, [<ProjectionParameter>] filter) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpUpdate (bulk.BulkUpdate().UpdateWhen(filter)) }
        | OpUpdate bulk -> { ctx with Operation = OpUpdate (bulk.UpdateWhen(filter)) }
        | _ -> failwith "Must add columns first."

    [<CustomOperation("identity", MaintainsVariableSpace=true)>]
    member this.IdentityColumn<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpUpdate (bulk.BulkUpdate().SetIdentityColumn(getPropertyName colExpr)) }
        | OpUpdate bulk -> { ctx with Operation = OpUpdate (bulk.SetIdentityColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."


type BulkUpdate(conn: IDbConnection) =
    inherit BulkUpdateBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpUpdate bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.Commit(conn, tx)
            | None -> bulk.Commit(conn)
        | _ -> failwith "Must add at least one column first."

type BulkUpdateTask(conn: IDbConnection) =
    inherit BulkUpdateBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpUpdate bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx)
            | None -> bulk.CommitAsync(conn)
        | _ -> failwith "Must add at least one column first."

type BulkUpdateAsync(conn: IDbConnection) =
    inherit BulkUpdateBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpUpdate bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx) |> Async.AwaitTask
            | None -> bulk.CommitAsync(conn) |> Async.AwaitTask
        | _ -> failwith "Must add at least one column first."

/// A bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn
/// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated.
let bulkUpdate conn = BulkUpdate(conn)

/// A task-based bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn
/// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated.
let bulkUpdateTask conn = BulkUpdateTask(conn)

/// An async bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn
/// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated.
let bulkUpdateAsync conn = BulkUpdateAsync(conn)


[<AbstractClass>]
type BulkUpsertBase(conn: IDbConnection) =
    let def = { Operation = OpNone; Transaction = None }

    member this.For (rows: seq<'T>, f: 'T -> Context<'T>) =
        { def with Operation = OpForCollection (BulkOperations().Setup().ForCollection(rows)) }

    member this.Yield _ =
        def

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, tx: IDbTransaction) =
        { ctx with Transaction = Some tx }

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, txMaybe: IDbTransaction option) =
        { ctx with Transaction = txMaybe }

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (ctx: Context<'T>, tbl) =
        match ctx.Operation with
        | OpForCollection bulk -> { ctx with Operation = OpWithTable(bulk.WithTable tbl) }
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnMap", MaintainsVariableSpace=true)>]
    member this.ColumnMap<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>, destination: string) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("matchTargetOn", MaintainsVariableSpace=true)>]
    member this.MatchTargetOn<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpUpsert (bulk.BulkInsertOrUpdate().MatchTargetOn(getPropertyName colExpr)) }
        | OpUpsert bulk -> { ctx with Operation = OpUpsert (bulk.MatchTargetOn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."

    [<CustomOperation("updateWhen", MaintainsVariableSpace=true)>]
    member this.UpdateWhen (ctx: Context<'T>, [<ProjectionParameter>] filter) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpUpsert (bulk.BulkInsertOrUpdate().UpdateWhen(filter)) }
        | OpUpsert bulk -> { ctx with Operation = OpUpsert (bulk.UpdateWhen(filter)) }
        | _ -> failwith "Must add columns first."

    [<CustomOperation("identity", MaintainsVariableSpace=true)>]
    member this.IdentityColumn<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpUpsert (bulk.BulkInsertOrUpdate().SetIdentityColumn(getPropertyName colExpr)) }
        | OpUpsert bulk -> { ctx with Operation = OpUpsert (bulk.SetIdentityColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."


type BulkUpsert(conn: IDbConnection) =
    inherit BulkUpsertBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpUpsert bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.Commit(conn, tx)
            | None -> bulk.Commit(conn)
        | _ -> failwith "Must add at least one column first."

type BulkUpsertTask(conn: IDbConnection) =
    inherit BulkUpsertBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpUpsert bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx)
            | None -> bulk.CommitAsync(conn)
        | _ -> failwith "Must add at least one column first."

type BulkUpsertAsync(conn: IDbConnection) =
    inherit BulkUpsertBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpUpsert bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx) |> Async.AwaitTask
            | None -> bulk.CommitAsync(conn) |> Async.AwaitTask
        | _ -> failwith "Must add at least one column first."

/// A bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
/// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least
/// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn)
/// will be evaluated.
let bulkUpsert conn = BulkUpsert(conn)

/// A task-based bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
/// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least
/// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn)
/// will be evaluated.
let bulkUpsertTask conn = BulkUpsertTask(conn)

/// An async bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
/// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least
/// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn)
/// will be evaluated.
let bulkUpsertAsync conn = BulkUpsertAsync(conn)


[<AbstractClass>]
type BulkDeleteBase(conn: IDbConnection) =
    let def = { Operation = OpNone; Transaction = None }

    member this.For (rows: seq<'T>, f: 'T -> Context<'T>) =
        { def with Operation = OpForCollection (BulkOperations().Setup().ForCollection(rows)) }

    member this.Yield _ =
        def

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, tx: IDbTransaction) =
        { ctx with Transaction = Some tx }

    [<CustomOperation("transaction", MaintainsVariableSpace = true)>]
    member this.Transaction (ctx: Context<'T>, txMaybe: IDbTransaction option) =
        { ctx with Transaction = txMaybe }

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (ctx: Context<'T>, tbl) =
        match ctx.Operation with
        | OpForCollection bulk -> { ctx with Operation = OpWithTable(bulk.WithTable tbl) }
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnMap", MaintainsVariableSpace=true)>]
    member this.ColumnMap<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>, destination: string) =
        match ctx.Operation with
        | OpWithTable bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | OpAddColumn bulk -> { ctx with Operation = OpAddColumn (bulk.AddColumn(getPropertyName colExpr, destination)) }
        | _ -> failwith "Must add table first."

    [<CustomOperation("matchTargetOn", MaintainsVariableSpace=true)>]
    member this.MatchTargetOn<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpDelete (bulk.BulkDelete().MatchTargetOn(getPropertyName colExpr)) }
        | OpDelete bulk -> { ctx with Operation = OpDelete (bulk.MatchTargetOn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."

    [<CustomOperation("deleteWhen", MaintainsVariableSpace=true)>]
    member this.DeleteWhen (ctx: Context<'T>, [<ProjectionParameter>] filter) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpDelete (bulk.BulkDelete().DeleteWhen(filter)) }
        | OpDelete bulk -> { ctx with Operation = OpDelete (bulk.DeleteWhen(filter)) }
        | _ -> failwith "Must add columns first."

    [<CustomOperation("identity", MaintainsVariableSpace=true)>]
    member this.IdentityColumn<'T, 'P> (ctx: Context<'T>, [<ProjectionParameter>] colExpr: Expression<Func<'T, 'P>>) =
        match ctx.Operation with
        | OpAddColumn bulk -> { ctx with Operation = OpDelete (bulk.BulkDelete().SetIdentityColumn(getPropertyName colExpr)) }
        | OpDelete bulk -> { ctx with Operation = OpDelete (bulk.SetIdentityColumn(getPropertyName colExpr)) }
        | _ -> failwith "Must add columns first."


type BulkDelete(conn: IDbConnection) =
    inherit BulkDeleteBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpDelete bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.Commit(conn, tx)
            | None -> bulk.Commit(conn)
        | _ -> failwith "Must add at least one column first."

type BulkDeleteTask(conn: IDbConnection) =
    inherit BulkDeleteBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpDelete bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx)
            | None -> bulk.CommitAsync(conn)
        | _ -> failwith "Must add at least one column first."

type BulkDeleteAsync(conn: IDbConnection) =
    inherit BulkDeleteBase(conn)

    member this.Run (ctx: Context<'T>) =
        match ctx.Operation with
        | OpDelete bulk ->
            match ctx.Transaction with
            | Some tx -> bulk.CommitAsync(conn, tx) |> Async.AwaitTask
            | None -> bulk.CommitAsync(conn) |> Async.AwaitTask
        | _ -> failwith "Must add at least one column first."

/// A bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK).
/// Notes: BulkDelete requires at least one MatchTargetOn property to be configured.
let bulkDelete conn = BulkDelete(conn)

/// A task-based bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK).
/// Notes: BulkDelete requires at least one MatchTargetOn property to be configured.
let bulkDeleteTask conn = BulkDeleteTask(conn)

/// An async bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK).
/// Notes: BulkDelete requires at least one MatchTargetOn property to be configured.
let bulkDeleteAsync conn = BulkDeleteAsync(conn)
