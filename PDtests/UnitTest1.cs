using System;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using PDtests;

namespace PDtests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            AnalyticEngineTests test = new AnalyticEngineTests();
            test.DropSocialDataTables();
            test._analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            test._analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 186 10% / 786 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;

            test._analyticEngine.submitQuerySQL("SELECT att2 FROM socialData evaluate using (Naive)", out table2);
            Assert.IsNotNull(table2);
            Assert.AreEqual(table2.Rows.Count, 4);

            Assert.IsTrue(test.dataRowEqual(table2.Rows[0], "786", 90));
            Assert.IsTrue(test.dataRowEqual(table2.Rows[1], "785", 50));
            Assert.IsTrue(test.dataRowEqual(table2.Rows[2], "185", 50));
            Assert.IsTrue(test.dataRowEqual(table2.Rows[3], "186", 10));

            test.DropSocialDataTables();
        }
    }
}
