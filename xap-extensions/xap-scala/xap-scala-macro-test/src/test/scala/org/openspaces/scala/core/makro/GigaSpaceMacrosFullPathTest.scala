package org.openspaces.scala.core.makro

import java.util.Date

import com.gigaspaces.metadata.index.SpaceIndexType
import org.junit.{After, Assert, Before, Test}
import org.openspaces.core.{GigaSpace, GigaSpaceConfigurer}
import org.openspaces.core.space.UrlSpaceConfigurer
import org.openspaces.scala.core.GigaSpaceMacroPredicateWrapper
import org.openspaces.scala.core.MacroDirectives._
import org.openspaces.scala.core.ScalaGigaSpacesImplicits.{QueryMacroDateImplicits, QueryMacroStringImplicits, ScalaEnhancedGigaSpaceWrapper}
import org.openspaces.scala.core.aliases.annotation._

import scala.beans.BeanProperty

case class MacroTestDataClass @SpaceClassConstructor() (
  
  @BeanProperty
  @SpaceId
  id: String = null,
  
  @BeanProperty
  @SpaceProperty(nullValue = "-1")
  @SpaceIndex(`type` = SpaceIndexType.EQUAL_AND_ORDERED)
  someNumber: Int = -1,
  
  @BeanProperty
  extra: String = null,
  
  @BeanProperty
  propertyForLike: String = null,
  
  @BeanProperty
  inner: MacroTestDataClass = null,
  
  @BeanProperty
  someDate: Date = null
  
)

class GigaSpaceMacrosFullPathTest {

  var configurer: UrlSpaceConfigurer = _
  var gigaSpace: GigaSpace = _
  var predicateSpace: GigaSpaceMacroPredicateWrapper = _
  
  @Before 
  def before() {
    configurer = new UrlSpaceConfigurer("/./testSpace")
    gigaSpace = new GigaSpaceConfigurer(configurer).create()
    predicateSpace = gigaSpace.predicate
  }
  
  @After
  def after() {
    if (configurer != null) {
      configurer.close()
    }
  } 
  
  @Test
  def test() {
    val written = MacroTestDataClass(id = "someId", 
                                     someNumber = 13, 
                                     propertyForLike = "some sentence",
                                     inner = MacroTestDataClass(extra = "extra stuff"),
                                     someDate = new Date(100))
    gigaSpace.write(written)
    
    testEquals(written)
    testNotEquals(written)
    testGreater(written)
    testGreaterEquals(written)
    testSmaller(written)
    testSmallerEquals(written)
    testAnd(written)
    testOr(written)
    testIsNull(written)
    testIsNotNull(written)
    testLike(written)
    testNotLike(written)
    testRLike(written)
    testNested(written)
    testDateGreater(written)
    testDateGreaterEquals(written)
    testDateSmaller(written)
    testDateSmallerEquals(written)
    testSelect(written)
    
    val written2 = MacroTestDataClass(id = "someId2", 
                                     someNumber = 14, 
                                     propertyForLike = "some sentence",
                                     inner = MacroTestDataClass(extra = "extra stuff"),
                                     someDate = new Date(100))
    gigaSpace.write(written2)
    
    testOrderBy(written, written2)
    testOrderByAscending(written, written2)
    testOrderByDescending(written, written2)
    
    testGroupBy(written, written2)
  }
  
  def testEquals(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.id == written.id
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.id == (written.id+"12")
    }
    Assert.assertNull(result)  
  }
  
  def testNotEquals(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.id != written.id
    }
    Assert.assertNull(result)        
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.id != (written.id+"12")
    }
    Assert.assertEquals(written, result)  
  }
 
  def testGreater(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber > written.someNumber
    }
    Assert.assertNull(result)        
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber > written.someNumber-1
    }
    Assert.assertEquals(written, result)  
  }

  def testGreaterEquals(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber >= written.someNumber
    }
    Assert.assertEquals(written, result)  
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber >= written.someNumber+1
    }
    Assert.assertNull(result)        
  }

  def testSmaller(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber < written.someNumber
    }
    Assert.assertNull(result)        
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber < written.someNumber+1
    }
    Assert.assertEquals(written, result)  
  }

  def testSmallerEquals(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber <= written.someNumber
    }
    Assert.assertEquals(written, result)  
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber <= written.someNumber-1
    }
    Assert.assertNull(result)        
  }
  
  def testAnd(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber == written.someNumber && data.id == written.id
    }
    Assert.assertEquals(written, result)  
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber == written.someNumber-1 && data.id == written.id
    }
    Assert.assertNull(result)        
  }

  def testOr(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber == written.someNumber-1 || data.id == written.id
    }
    Assert.assertEquals(written, result)  
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someNumber == written.someNumber-1 || data.id == written.id+"11"
    }
    Assert.assertNull(result)        
  }
  
  def testIsNull(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.extra eq null
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.id eq null
    }
    Assert.assertNull(result)  
  }
  
  def testIsNotNull(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.id ne null
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.extra ne null
    }
    Assert.assertNull(result)  
  }
  
  def testLike(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.propertyForLike like "some%"
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.propertyForLike like "notSome%"
    }
    Assert.assertNull(result)  
  }

  def testNotLike(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.propertyForLike notLike "notSome%"
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.propertyForLike notLike "some%"
    }
    Assert.assertNull(result)  
  }
  
  def testRLike(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.propertyForLike rlike "some.*"
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.propertyForLike rlike "notSome.*"
    }
    Assert.assertNull(result)  
  }
  
  def testNested(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.inner.extra == written.inner.extra
    }
    Assert.assertEquals(written, result)  
    
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.inner.extra != written.inner.extra
    }
    Assert.assertNull(result)  
  }
  
  def testDateGreater(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate > written.someDate
    }
    Assert.assertNull(result)        
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate > new Date(written.someDate.getTime() - 1)
    }
    Assert.assertEquals(written, result)  
  }

  def testDateGreaterEquals(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate >= written.someDate
    }
    Assert.assertEquals(written, result)  
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate >= new Date(written.someDate.getTime() + 1)
    }
    Assert.assertNull(result)        
  }

  def testDateSmaller(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate < written.someDate
    }
    Assert.assertNull(result)        
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate < new Date(written.someDate.getTime() + 1)
    }
    Assert.assertEquals(written, result)  
  }

  def testDateSmallerEquals(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate <= written.someDate
    }
    Assert.assertEquals(written, result)  
    result = predicateSpace.read { data: MacroTestDataClass =>
      data.someDate <= new Date(written.someDate.getTime() - 1)
    }
    Assert.assertNull(result)        
  }  

  def testSelect(written: MacroTestDataClass) {
    var result = predicateSpace.read { data: MacroTestDataClass =>
      select(data.propertyForLike, data.someNumber)
      data.id == written.id
    }
    Assert.assertNull(result.id)
    Assert.assertEquals(written.propertyForLike, result.propertyForLike)
    Assert.assertEquals(written.someNumber, result.someNumber)
  }  
  
  def testOrderBy(written: MacroTestDataClass, written2: MacroTestDataClass) {
    var result: Array[MacroTestDataClass] = predicateSpace.readMultiple { data: MacroTestDataClass =>
      orderBy(data.id)
      data.extra eq null
    }
    
    Assert.assertEquals(2, result.length)
    Assert.assertEquals(written.id, result(0).id)
    Assert.assertEquals(written2.id, result(1).id)
  }  
  
  def testOrderByAscending(written: MacroTestDataClass, written2: MacroTestDataClass) {
    var result: Array[MacroTestDataClass] = predicateSpace.readMultiple { data: MacroTestDataClass =>
      orderBy(data.id).ascending
      data.extra eq null
    }
    
    Assert.assertEquals(2, result.length)
    Assert.assertEquals(written.id, result(0).id)
    Assert.assertEquals(written2.id, result(1).id)
  }  
 
  def testOrderByDescending(written: MacroTestDataClass, written2: MacroTestDataClass) {
    var result: Array[MacroTestDataClass] = predicateSpace.readMultiple { data: MacroTestDataClass =>
      orderBy(data.id).descending
      data.extra eq null
    }
    
    Assert.assertEquals(2, result.length)
    Assert.assertEquals(written2.id, result(0).id)
    Assert.assertEquals(written.id, result(1).id)
  }  
  
  def testGroupBy(written: MacroTestDataClass, written2: MacroTestDataClass) {
    var result: Array[MacroTestDataClass] = predicateSpace.readMultiple { data: MacroTestDataClass =>
      groupBy(data.id)
      data.extra eq null
    }
    Assert.assertEquals(Set(written, written2), result.toSet)
    
    result = predicateSpace.readMultiple { data: MacroTestDataClass =>
      groupBy(data.propertyForLike)
      data.extra eq null
    }
    Assert.assertEquals(1, result.length)
    Assert.assertTrue(result(0) == written || result(0) == written2)
  }  
  
}