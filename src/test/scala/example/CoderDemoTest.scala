package example

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._
import com.spotify.scio.coders._
import CoderAssertions._
import org.scalatest.Inside

// Most of those tests are taken from
// https://github.com/spotify/scio/blob/master/scio-test/src/test/scala/com/spotify/scio/coders/CoderTest.scala
class CoderDemoTest extends PipelineSpec with Inside {

  /**
    * This should be easy. Coder[UserBean] simply check that UserBean is a Java Bean.
    * However, it is important that Coder[UserBean] does NOT fallback.
    * In other words, javaBeanCoder must higher precedence than Coder.fallback
    * @see: https://github.com/spotify/scio/blob/master/scio-core/src/main/scala/com/spotify/scio/coders/instances/JavaCoders.scala#L150
    */
  "Coders" should "Serialize Java beans using a Schema Coder" in {
    val javaUser = new UserBean("Julien", 35)
    javaUser coderShould roundtrip()
    javaUser coderShould notFallback()
    inside (Coder[UserBean]) { case Beam(bc) =>
      bc shouldBe a [org.apache.beam.sdk.schemas.SchemaCoder[UserBean]]
    }
  }

  /**
    * Not Coder is defined for the following case classes.
    * Scio should derive them at compile time
    */
  it should "derive coders for product types" in {
    DummyCC("dummy") coderShould notFallback()
    DummyCC("") coderShould notFallback()
    ParameterizedDummy("dummy") coderShould notFallback()
    MultiParameterizedDummy("dummy", 2) coderShould notFallback()
    val userId: UserId = UserId(Array[Byte](1, 2, 3, 4))
    val user: User = User(userId, "johndoe", "johndoe@spotify.com")
    user coderShould notFallback()
    (1, "String", List[Int]()) coderShould notFallback()
    val ds = (1 to 10).map(_ => DummyCC("dummy")).toList
    ds coderShould notFallback()
  }

  /**
    * Not Coder is defined for the following hierarchy.
    * Scio should derive them at compile time
    */
  it should "derive coders for sealed class hierarchies" in {
    val ta: Top = TA(1, "test")
    val tb: Top = TB(4.2)
    ta coderShould notFallback()
    tb coderShould notFallback()
    (123, "hello", ta, tb, List(("bar", 1, "foo"))) coderShould notFallback()
  }

  /**
    * Here the a Coder[CaseClassWithExplicitCoder] is provided in the companion object,
    * therefore, we should just use it.
    * It's important that we do not derive a Coder[CaseClassWithExplicitCoder], nor do we use a fallback.
    */
  it should "only derive Coder if no coder exists" in {
    CaseClassWithExplicitCoder(1, "hello") coderShould notFallback()
    Coder[CaseClassWithExplicitCoder] should
      ===(CaseClassWithExplicitCoder.caseClassWithExplicitCoderCoder)
  }

  /**
    * When no other option is available, the fallback coder is used.
    * The compilation must not fail but the compiler issue an "info" telling
    * the user they should provide a better Coder.
    * Ideally that info should be a warning.
    * IMPORTANT (and difficult). The warning message must be issues
    * _ONLY_ if the fallback coder is used
    * (No warning if the compiler "considers" the fallback but ultimately picks another Coder)
    * ^^^^
    * This is important and was *really* hard to get right.
    * @see https://spotify.github.io/scio/internals/Coders.html for complete doc.
    */
  it should "only fallback if no coder exists" in {
    val loc = java.util.Locale.CANADA
    // [info]  Warning: No implicit Coder found for the following type:
    // [info]
    // [info]    >> java.util.Locale
    // [info]
    // [info]  using Kryo fallback instead.
    loc coderShould fallback()

    // The Coder ParameterizedDummy(loc) loc will be derived properly even though loc uses a fallback.
    // The compiler still issues a warning:
    // [info]  Warning: No implicit Coder found for the following type:
    // [info]
    // [info]    >> java.util.Locale
    // [info]
    // [info]  using Kryo fallback instead.
    // [info]
    // [info]     ParameterizedDummy(loc) coderShould notFallback()
    ParameterizedDummy(loc) coderShould notFallback()
  }

}

final case class UserId(bytes: Seq[Byte])
final case class User(id: UserId, username: String, email: String)

sealed trait Top
final case class TA(anInt: Int, aString: String) extends Top
final case class TB(anDouble: Double) extends Top

case class DummyCC(s: String)
case class ParameterizedDummy[A](value: A)
case class MultiParameterizedDummy[A, B](valuea: A, valueb: B)

case class CaseClassWithExplicitCoder(i: Int, s: String)
object CaseClassWithExplicitCoder {
  import org.apache.beam.sdk.coders.{AtomicCoder, StringUtf8Coder, VarIntCoder}
  import java.io.{InputStream, OutputStream}
  implicit val caseClassWithExplicitCoderCoder: Coder[CaseClassWithExplicitCoder] =
    Coder.beam(new AtomicCoder[CaseClassWithExplicitCoder] {
      val sc = StringUtf8Coder.of()
      val ic = VarIntCoder.of()
      def encode(value: CaseClassWithExplicitCoder, os: OutputStream): Unit = {
        ic.encode(value.i, os)
        sc.encode(value.s, os)
      }
      def decode(is: InputStream): CaseClassWithExplicitCoder = {
        val i = ic.decode(is)
        val s = sc.decode(is)
        CaseClassWithExplicitCoder(i, s)
      }
    })
}


