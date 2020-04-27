package ThetaJoinTest.Skewed

class SkewedLarge extends Skewed {
  test("ThetaJoin.ineq_joinTestSkewed4k1PartitionLarger") {
    testSkewed()(thetaJoin1, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k1PartitionSmaller") {
    testSkewed()(thetaJoin1, "<")
  }


  test("ThetaJoin.ineq_joinTestSkewed4k16PartitionsLarger") {
    testSkewed()(thetaJoin16, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k16PartitionsSmaller") {
    testSkewed()(thetaJoin16, "<")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k128PartitionsLarger") {
    testSkewed()(thetaJoin128, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k128PartitionsSmaller") {
    testSkewed()(thetaJoin128, "<")
  }
}
