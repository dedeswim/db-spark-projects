package ThetaJoinTest.Skewed

class SkewedSmall extends Skewed {
  test("1PartitionLarger") {
    testSkewed(1000)(thetaJoin1, ">")
  }

  test("1PartitionSmaller") {
    testSkewed(1000)(thetaJoin1, "<")
  }

  test("2PartitionsLarger") {
    testSkewed(1000)(thetaJoin2, ">")
  }

  test("2PartitionsSmaller") {
    testSkewed(1000)(thetaJoin2, "<")
  }

  test("16PartitionsLarger") {
    testSkewed(1000)(thetaJoin16, ">")
  }

  test("16PartitionsSmaller") {
    testSkewed(1000)(thetaJoin16, "<")
  }

  test("128PartitionsLarger") {
    testSkewed(1000)(thetaJoin128, ">")
  }

  test("128PartitionsSmaller") {
    testSkewed(1000)(thetaJoin128, "<")
  }
}
