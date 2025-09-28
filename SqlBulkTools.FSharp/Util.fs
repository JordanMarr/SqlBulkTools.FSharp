module SqlBulkTools.Util

open System
open System.Linq.Expressions
open System.Data

let getPropertyName<'T, 'TProp> (expr: Expression<Func<'T, 'TProp>>) : string =
    match expr.Body with
    | :? MemberExpression as memberExpr -> memberExpr.Member.Name
    | _ -> failwith "Expression is not a member access"
