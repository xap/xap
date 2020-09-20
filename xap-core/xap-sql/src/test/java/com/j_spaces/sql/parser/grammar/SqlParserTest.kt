package com.j_spaces.sql.parser.grammar

import com.j_spaces.sql.parser.*
import org.junit.Assert
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlParserTest {

    private val selectData = listOf(
            "select 1 as foo from a b" to Select(NoneQuantifier, SomeColumns(listOf(LiteralColumn(IntLit(1), "foo"))), listOf(TableName("a", "b"))),
            "select null as foo from a b" to Select(NoneQuantifier, SomeColumns(listOf(LiteralColumn(NullLit, "foo"))), listOf(TableName("a", "b"))),
            "select * from a" to Select(NoneQuantifier, AllColumns, listOf(TableName("a"))),
            "select all * from a b" to Select(AllQuantifier, AllColumns, listOf(TableName("a", "b"))),
            "select distinct * from a, b" to Select(DistinctQuantifier, AllColumns, listOf(TableName("a"), TableName("b"))),
            "select * from t" to Select(NoneQuantifier, AllColumns, listOf(TableName("t"))),
            "select foo from t" to Select(NoneQuantifier, SomeColumns(listOf(AColumn("foo"))), listOf(TableName("t"))),
            "select foo, bar from t" to Select(NoneQuantifier,
                    SomeColumns(listOf(AColumn("foo"), AColumn("bar"))), listOf(TableName("t"))),
            "select `foo` from t" to Select(NoneQuantifier, SomeColumns(listOf(AColumn("foo"))), listOf(TableName("t"))),
            "select foo `bar` from t" to Select(columns = SomeColumns(listOf(AColumn("foo", "bar"))), from = listOf(TableName("t"))),
            "select `foo` as bar from t" to Select(columns = SomeColumns(listOf(AColumn("foo", "bar"))), from = listOf(TableName("t"))),
            "select max (foo) as bar from t" to Select(columns = SomeColumns(listOf(FunctionColumn("max", AColumn("foo"), "bar"))), from = listOf(TableName("t"))),
            "select * from a where 1 = 1" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(IntLit(1), "=", IntLit(1))),
            "select * from a where 1 = 1 and 2 > 3" to Select(
                    NoneQuantifier, AllColumns, listOf(TableName("a")),
                    And(listOf(CondOp(IntLit(1), "=", IntLit(1)), CondOp(IntLit(2), ">", IntLit(3))))),
            "select * from a where 1 = 1 or 2 > 3" to Select(
                    NoneQuantifier, AllColumns, listOf(TableName("a")),
                    Or(listOf(CondOp(IntLit(1), "=", IntLit(1)), CondOp(IntLit(2), ">", IntLit(3))))),
            "select * from a where 1 = true" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(IntLit(1), "=", BooleanLit(true))),
            "select * from a where ? = ?" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(PreparedLit(1), "=", PreparedLit(2))),
            "select * from a where a = b" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(TableRef("a"), "=", TableRef("b"))),
            "select * from a where 1L = -1.0" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(LongLit(1), "=", FloatLit(-1.0f))),
            "select * from a where '20/09/1999' = -1.0" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(DateLit("20/09/1999"), "=", FloatLit(-1.0f))),
            "select * from a where rownum = 1" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondRowNum("=", 1)),
            "select * from a where foo(a.b) = foo(0)" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(FunctionCall("foo", listOf(TableRef("a.b"))), "=", FunctionCall("foo", listOf(IntLit(0))))),
            "select * from a where 2 = (select * from a where 1 = 1)" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondOp(IntLit(2), "=",
                            Select(NoneQuantifier, AllColumns, listOf(TableName("a")), CondOp(IntLit(1), "=", IntLit(1))))),
            "select * from a where foo in (1, 2)" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondInList(TableRef("foo"), false, listOf(IntLit(1), IntLit(2)))),
            "select * from a where foo not in (select * from b)" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondInSelect(TableRef("foo"), true, Select(NoneQuantifier, AllColumns, listOf(TableName("b"))))),
            "select * from a where foo is not null" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondIsNull(TableRef("foo"), true)),
            "select * from a where foo between 5 and b" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondBetween(TableRef("foo"), IntLit(5), TableRef("b"))),
            "select * from a where b text:search ?" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondRelation(TableRef("b"), "text:search", PreparedLit(1))),
            "select * from a where [*].foo[*].bar in (1, 2)" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondInList(CollectionPath(listOf(Contains, PathElement("foo"), Contains, PathElement("bar"))), false, listOf(IntLit(1), IntLit(2)))),
            "select * from a where rownum = 1 group by foo" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondRowNum("=", 1), listOf(AColumn("foo"))),
//            OrderByColumn(col=AColumn(name=foo, alias=null), direction=ASC, nulls=NOTHING)
            "select * from a where rownum = 1 order by foo asc" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondRowNum("=", 1), listOf(), listOf(OrderByColumn(AColumn("foo")))),
            "select * from a where foo between 5 and b for update" to Select(NoneQuantifier, AllColumns, listOf(TableName("a")),
                    CondBetween(TableRef("foo"), IntLit(5), TableRef("b")), listOf(), listOf(), true),

            )

    @TestFactory
    fun testSelects() = selectData
            .map { (input, expected) ->
                DynamicTest.dynamicTest("when I parse '$input' then I get $expected") {
                    val parser = SqlParser(input.reader())
                    val statement = parser.parseStatement()
                    Assert.assertEquals(expected, statement)
                }
            }

}