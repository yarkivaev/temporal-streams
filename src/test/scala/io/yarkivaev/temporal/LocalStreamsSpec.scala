package io.yarkivaev.temporal

class LocalStreamsSpec extends munit.FunSuite:
  def point(t: Long, value: Double = 1, id: String = "") = Observation(if id.isEmpty then s"e$t" else id, "power", t, value)
  test("nearest preceding stream, distant arrivals and idle eviction") {
    val a = LocalStreams.empty(10, 100).route(point(10), 0)
    val b = a.state.route(point(15), 1)
    assertEquals(a.stream.id, b.stream.id)
    val c = b.state.route(point(50), 2)
    assertNotEquals(c.stream.id, b.stream.id)
    val d = c.state.route(point(16), 3)
    assertEquals(d.stream.id, b.stream.id)
    assertEquals(d.state.evict(102).streams.size, 1)
  }
  test("first write wins, duplicate event, and channels grouped at the same timestamp") {
    val h = History.empty.accept(point(10, 1, "a"), Conflict.first).accept(point(10, 2, "b"), Conflict.first)
    assertEquals(h.at(10).map(_.value), Vector(1.0))
    assertEquals(h.accept(point(20, 3, "a"), Conflict.first), h)
    val more = h.accept(Observation("c", "voltage", 10, 100), Conflict.first)
    assertEquals(more.at(10).size, 2)
  }
  test("bounded local history includes neighbours without replaying to present") {
    val h = Vector(0L, 10L, 20L, 10000L).foldLeft(History.empty)((h,t) => h.accept(point(t), Conflict.first))
    assertEquals(h.context(10, 10).map(_.time), Vector(0L,10L,20L))
    assertEquals(h.context(11, 11).map(_.time), Vector(10L,20L))
  }
  test("pending work survives idle eviction and is cleared only by acknowledgement") {
    val a = LocalStreams.empty(10, 100).route(point(10), 0)
    val expired = a.state.evict(1000)
    assertEquals(expired.streams.size, 0)
    assertEquals(expired.pending.size, 1)
    assertEquals(expired.acknowledge(a.work.id).pending.size, 0)
  }
  test("merging requires explicit equivalence and never removes pending work") {
    val a = LocalStreams.empty(10,100).route(point(0),0).state.route(point(50),1).state
    val ids = a.streams.map(_.id)
    assertEquals(a.merge(ids(0),ids(1),false).streams.size,2)
    val converged = a.copy(streams = a.streams.map(_.copy(position = 50)))
    val merged = converged.merge(ids(0),ids(1),true)
    assertEquals(merged.streams.size,1)
    assertEquals(merged.pending,a.pending)
  }
  test("operator marks follow applicable descendants and expanded boundaries; cuts remain") {
    val old = Segment("old",10,30,"off",Set.empty)
    val marks = Vector(Annotation("reason", "old", "repair", Set("off")))
    val revised = Vector(Segment("left",8,18,"off",Set("old")),Segment("heat",18,22,"heat",Set("old")),Segment("right",22,35,"off",Set("old")))
    val result = Overlay(revised, marks, Vector(12))
    assertEquals(result.map(_.segment.start),Vector(8L,12L,18L,22L))
    assertEquals(result.filter(_.segment.kind == "off").map(_.annotations),Vector(Vector("reason"),Vector("reason"),Vector("reason")))
    assertEquals(result.find(_.segment.kind == "heat").get.annotations,Vector.empty)
    assertEquals(Overlay(Vector(old),marks,Vector.empty).head.annotations,Vector("reason"))
  }
