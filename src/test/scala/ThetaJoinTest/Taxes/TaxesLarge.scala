package ThetaJoinTest.Taxes

class TaxesLarge extends Taxes {
  test("1PartitionLarger") {
    testTaxes()(thetaJoin1, ">")
  }

  test("1PartitionSmaller") {
    testTaxes()(thetaJoin1, "<")
  }

  test("16PartitionsLarger") {
    testTaxes()(thetaJoin16, ">")
  }

  test("16PartitionsSmaller") {
    testTaxes()(thetaJoin16, "<")
  }

  test("128PartitionsLarger") {
    testTaxes()(thetaJoin128, ">")
  }

  test("128PartitionsSmaller") {
    testTaxes()(thetaJoin128, "<")
  }
}
