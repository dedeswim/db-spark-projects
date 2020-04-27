package ThetaJoinTest.Taxes

class TaxesLarge extends Taxes {
  test("ThetaJoin.ineq_joinTestTax4k1PartitionLarger") {
    testTaxes()(thetaJoin1, ">")
  }

  test("ThetaJoin.ineq_joinTestTax4k1PartitionSmaller") {
    testTaxes()(thetaJoin1, "<")
  }

  test("ThetaJoin.ineq_joinTestTax4k16PartitionsLarger") {
    testTaxes()(thetaJoin16, ">")
  }

  test("ThetaJoin.ineq_joinTestTax4k16PartitionsSmaller") {
    testTaxes()(thetaJoin16, "<")
  }

  test("ThetaJoin.ineq_joinTestTax4k128PartitionsLarger") {
    testTaxes()(thetaJoin128, ">")
  }

  test("ThetaJoin.ineq_joinTestTax4k128PartitionsSmaller") {
    testTaxes()(thetaJoin128, "<")
  }
}
