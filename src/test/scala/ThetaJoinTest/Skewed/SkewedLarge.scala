package ThetaJoinTest.Skewed

class SkewedLarge extends Skewed {
  test("1PartitionLarger") {
    testSkewed()(thetaJoin1, ">")
  }

  test("1PartitionSmaller") {
    testSkewed()(thetaJoin1, "<")
  }

  test("16PartitionsLarger") {
    testSkewed()(thetaJoin16, ">")
  }

  test("16PartitionsSmaller") {
    testSkewed()(thetaJoin16, "<")
  }

  test("128PartitionsLarger") {
    testSkewed()(thetaJoin128, ">")
  }

  test("128PartitionsSmaller") {
    testSkewed()(thetaJoin128, "<")
  }
}
