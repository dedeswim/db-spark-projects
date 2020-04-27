package ThetaJoinTest.Skewed

class SkewedSmall extends Skewed {
  test("ThetaJoin.ineq_joinTestSkewed1k1PartitionLarger") {
    testSkewed(1000)(thetaJoin1, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k1PartitionSmaller") {
    testSkewed(1000)(thetaJoin1, "<")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k2PartitionsLarger") {
    testSkewed(1000)(thetaJoin2, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k2PartitionsSmaller") {
    testSkewed(1000)(thetaJoin2, "<")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k16PartitionsLarger") {
    testSkewed(1000)(thetaJoin16, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k16PartitionsSmaller") {
    testSkewed(1000)(thetaJoin16, "<")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k128PartitionsLarger") {
    testSkewed(1000)(thetaJoin128, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed1k128PartitionsSmaller") {
    testSkewed(1000)(thetaJoin128, "<")
  }
}
