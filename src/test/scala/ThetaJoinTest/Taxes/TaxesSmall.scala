package ThetaJoinTest.Taxes

class TaxesSmall extends Taxes {
  test("1PartitionLarger") {
    testTaxes(1000)(thetaJoin1, ">")
  }

  test("1PartitionSmaller") {
    testTaxes(1000)(thetaJoin1, "<")
  }

  test("2PartitionsLarger") {
    testTaxes(1000)(thetaJoin2, ">")
  }

  test("2PartitionsSmaller") {
    testTaxes(1000)(thetaJoin2, "<")
  }

  test("16PartitionsLarger") {
    testTaxes(1000)(thetaJoin16, ">")
  }

  test("16PartitionsSmaller") {
    testTaxes(1000)(thetaJoin16, "<")
  }

  test("128PartitionsLarger") {
    testTaxes(1000)(thetaJoin128, ">")
  }

  test("128PartitionsSmaller") {
    testTaxes(1000)(thetaJoin128, "<")
  }
}
