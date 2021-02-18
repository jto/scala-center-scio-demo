package example

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._
import com.spotify.scio.schemas._
import CoderAssertions._
import org.scalatest.Inside
import scala.jdk.CollectionConverters._

class SchemaDemoTest extends PipelineSpec with Inside {
  import TestData._

  /**
    * To.safe[A, B]` convert instances of A into instances of B.
    * It derives / find implicitly the Schema[A] and the Schema[B] and compare them at compile time.
    * Given their schemas, it's fairly easy to implement a conversion function.
    * Note that the names and types of the fields in each class is considered, but not their order
    * See the definition of From0 and To0 for an illustration of this.
    */
  "To" should "convert compatible classes" in {
    val from: List[From0] =
      (1 to 10).map { i =>
        From0(1, s"from0_$i", From1((20 to 30).toList.map(_.toLong), s"from1_$i"))
      }.toList

    val to: List[To1] =
      from.map { case From0(i, s, From1(xs, q)) =>
        To1(s, To0(q, xs), i)
      }

    val converter = To.safe[From0, To1]
    from.map(f => converter.convert(f)) should contain theSameElementsInOrderAs to
  }

  /**
    * The compiler derive a Schema[TinyTo] and a Schema[From0] and compare them (at compile time)
    * Here TinyTo and From0 are not compatible therefore, the code does not compile
    */
  it should "check classes compatibility at compile time" in {

    """To.safe[TinyTo, From0]""" shouldNot compile
    """To.safe[From0, CompatibleAvroTestRecord]""" shouldNot compile
  }

  /**
    * Beam provide a Schema instance for Java Beans
    * The implementation is non-trivial so ideally we'd want to reuse it.
    * (But thinking about it maybe it isn't that hard and the right think to do anyway ?)
    * Thanks to `eval` we can resolve the implicit and access the Schema generated by Beam
    * @see https://beam.apache.org/releases/javadoc/2.27.0/org/apache/beam/sdk/schemas/JavaBeanSchema.html
    * @see https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/schemas/JavaBeanSchema.java
    */
  it should "work support Java beans" in {
    val javaUsers: IndexedSeq[UserBean] =
      (1 to 10).map(i => new UserBean(s"user$i", 20 + i))

    val expectedJavaCompatUsers: IndexedSeq[JavaCompatibleUser] =
      javaUsers.map(j => JavaCompatibleUser(j.getName, j.getAge))

    val converter = To.safe[UserBean, JavaCompatibleUser]

    javaUsers.map(f => converter.convert(f)) should contain theSameElementsInOrderAs expectedJavaCompatUsers

    """To.safe[JavaCompatibleUser, From0]""" shouldNot compile
  }

  /**
    * Avro SpecificRecord is also supported
    */
  it should "work support Avro's SpecificRecord" in {
    import com.spotify.scio.avro

    val avroWithNullable: List[avro.TestRecord] =
      (1 to 10).map { i =>
        new avro.TestRecord(i, i, null, null, false, null, List[CharSequence](s"value_$i").asJava)
      }.toList

    val expectedAvro: List[CompatibleAvroTestRecord] =
      avroWithNullable.map { r =>
        CompatibleAvroTestRecord(
          Option(r.getIntField.toInt),
          Option(r.getLongField.toLong),
          Option(r.getStringField).map(_.toString),
          r.getArrayField.asScala.toList.map(_.toString)
        )
      }

    val converter = To.safe[avro.TestRecord, CompatibleAvroTestRecord]
    avroWithNullable.map(f => converter.convert(f)) should contain theSameElementsInOrderAs expectedAvro

    """To.safe[avro.TestRecord, From0]""" shouldNot compile
  }
}

object TestData {
  case class From1(xs: List[Long], q: String)
  case class From0(i: Int, s: String, e: From1)

  case class To0(q: String, xs: List[Long])
  case class To1(s: String, e: To0, i: Int)

  case class JavaCompatibleUser(name: String, age: Int)

  case class CompatibleAvroTestRecord(
    int_field: Option[Int],
    long_field: Option[Long],
    string_field: Option[String],
    array_field: List[String]
  )
}