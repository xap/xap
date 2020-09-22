package com.j_spaces.sql.parser


sealed class Exp
data class Select(val quantifier: SelectQuantifier = NoneQuantifier, val columns: SelectColumns, val from: List<TableName>, val where: Exp? = null, val groupBy: List<SelectColumn> = listOf(), val orderBy: List<OrderByColumn> = listOf(), val forUpdate: Boolean = false) : Exp()


sealed class SelectQuantifier
object DistinctQuantifier : SelectQuantifier()
object AllQuantifier : SelectQuantifier()
object NoneQuantifier : SelectQuantifier()


sealed class SelectColumns
object AllColumns : SelectColumns()
data class SomeColumns(val columns: List<SelectColumn>) : SelectColumns()


sealed class SelectColumn
data class UIDColumn(val alias: String? = null) : SelectColumn()
data class AColumn(val name: String, val alias: String? = null) : SelectColumn()
data class LiteralColumn(val lit: Literal, val alias: String? = null) : SelectColumn()
data class FunctionColumn(val name: String, val column: SelectColumn, val alias: String? = null) : SelectColumn()


data class OrderByColumn(val col: SelectColumn, val direction: OrderByDirection = OrderByDirection.ASC, val nulls: OrderByNulls = OrderByNulls.NOTHING)
enum class OrderByDirection {
    ASC,
    DESC
}

enum class OrderByNulls {
    NULLS_LAST,
    NULLS_FIRST,
    NOTHING
}

data class TableName(val name: String, val alias: String? = null)

sealed class Literal : Exp()
data class IntLit(val value: Int) : Literal()
data class LongLit(val value: Long) : Literal()
data class FloatLit(val value: Float) : Literal()
data class DateLit(val value: String) : Literal()
data class StringLit(val value: String) : Literal()
data class BooleanLit(val value: Boolean) : Literal()
data class PreparedLit(val pos: Int) : Literal()
object NullLit : Literal()

data class CollectionPath(val path: List<CollectionPathElement>) : Literal()

sealed class CollectionPathElement
object Contains : CollectionPathElement()
data class PathElement(val path: String) : CollectionPathElement()


data class And(val expressions: List<Exp>) : Exp()
data class Or(val expressions: List<Exp>) : Exp()
data class TableRef(val name: String) : Exp()
data class FunctionCall(val name: String, val args: List<Exp>) : Exp()

sealed class Cond : Exp()
data class CondOp(val exp1: Exp, val op: String, val exp2: Exp) : Cond()
data class CondRelation(val exp1: Exp, val op: String, val exp2: PreparedLit) : Cond()
data class CondRowNum(var op: String, val value: Int) : Cond()
data class CondRowNumRange(var from: Int, val to: Int) : Cond()
data class CondIsNull(val exp: Exp, val neg: Boolean) : Cond()
data class CondBetween(val exp: Exp, val left: Exp, val right: Exp) : Cond()

sealed class CondIn : Cond()
data class CondInSelect(val exp1: Exp, val neg: Boolean, val exp2: Exp) : CondIn()
data class CondInList(val exp1: Exp, val neg: Boolean, val expressions: List<Exp>) : CondIn()
