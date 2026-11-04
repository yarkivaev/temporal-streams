package io.yarkivaev.temporal

import cats.effect.IO
import cats.effect.unsafe.implicits.global
class WorkerSpec extends munit.FunSuite:
  test("worker commits local output before acknowledging and skips absent context") {
    var calls = Vector.empty[String]
    val storage = new Processing[IO,String]:
      def context(work: Work) = IO { calls :+= "read"; Some(Vector(Observation("x","p",10,1))) }
      def classify(work: Work, points: Vector[Observation]) = IO { calls :+= "classify"; Vector("segment") }
      def commit(work: Work, result: Vector[String]) = IO { calls :+= "commit" }
      def acknowledge(work: Work) = IO { calls :+= "ack" }
    Worker(storage).run(Work(1,1,10,10)).unsafeRunSync()
    assertEquals(calls,Vector("read","classify","commit","ack"))
  }
  test("failed commit does not acknowledge work") {
    var ack = false
    val storage = new Processing[IO,String]:
      def context(work: Work) = IO.pure(Some(Vector.empty))
      def classify(work: Work, points: Vector[Observation]) = IO.pure(Vector.empty)
      def commit(work: Work, result: Vector[String]) = IO.raiseError(new RuntimeException("disk"))
      def acknowledge(work: Work) = IO { ack = true }
    assert(Worker(storage).run(Work(1,1,10,10)).attempt.unsafeRunSync().isLeft)
    assertEquals(ack,false)
  }
  test("missing historical context does not invoke classifier") {
    var classified = false
    val storage = new Processing[IO,String]:
      def context(work: Work) = IO.pure(None)
      def classify(work: Work, points: Vector[Observation]) = IO { classified = true; Vector.empty }
      def commit(work: Work, result: Vector[String]) = IO.unit
      def acknowledge(work: Work) = IO.unit
    Worker(storage).run(Work(1,1,10,10)).unsafeRunSync()
    assertEquals(classified,false)
  }
