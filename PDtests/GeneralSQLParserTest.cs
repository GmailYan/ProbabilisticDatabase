using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProbabilisticDatabase.Src.ControllerPackage;
using ProbabilisticDatabase.Src.DatabaseEngine;

namespace PDtests
{
    [TestClass]
    public class GeneralSQLParserTest
    {
        readonly AnalyticEngine _analyticEngine = new AnalyticEngine();
        readonly StandardDatabase _underlineDatabase = new StandardDatabase();

        [TestMethod]
        public void TestUnion()
        {
            SetupFinancialWorld();
            var sql = "select * from (Select att1 as company from CompanyStock) as R1 union (select att1 as company from CompanyInfo) as R2 evaluate using (extensional)";
            DataTable table;
            _analyticEngine.submitQuerySQL(sql,out table);
        }

        [TestMethod]
        public void TestDifference()
        {
            SetupFinancialWorld();
            var sql = "select * from (Select att1 as company from CompanyStock) where not exists (select att1 as company from CompanyInfo) evaluate using (extensional)";
            DataTable table;
            _analyticEngine.submitQuerySQL(sql, out table);
        }

        [TestMethod]
        public void TestJoin()
        {
            SetupFinancialWorld();

            var sql = "select company1,Price,HQ from (Select att1 as company1,att2 as Price from CompanyStock) as R1 join (select att1 as company2,att2 as HQ from CompanyInfo) as R2 on company1=company2 evaluate using (extensional)";
            DataTable table;
            _analyticEngine.submitQuerySQL(sql, out table);
        }

        [TestMethod]
        public void TestSelectSingleTable()
        {
            SetupFinancialWorld();

            var sql = "select att1 as company,att2 as price from CompanyStock evaluate using (extensional)";
            DataTable table;
            _analyticEngine.submitQuerySQL(sql, out table);
        }

        [TestMethod]
        public void TestSelectSingleTableWhere()
        {
            SetupFinancialWorld();

            var sql = "select * from CompanyStock where att1='Google' evaluate using (extensional)";
            DataTable table;
            _analyticEngine.submitQuerySQL(sql, out table);
        }

        private void SetupFinancialWorld()
        {
            //clean the environment before start actual BenchmarkTest
            DropFinancialWorld();

            //stock table
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (Google, " +
                                     "PROBABLY 920.1103333841 25% / 839.8293413259 50% / 766.5529849641 25% )");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (Apple," +
                                     "Probably 462.4615296 25% / 458.1030132 50% / 453.7855740875 25% )");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (Microsoft, " +
                                     "Probably 28.1324924976 25% / 28.1099955024 50% / 28.0875164976 25% ) PROBABLY 100%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (IBM_1, " +
                                     "Probably 258.2503 25% / 223.03435 50% / 192.620575 25% ) PROBABLY 50%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (IBM_2, " +
                                     "Probably 212.25447632 25% / 211.066564 50% / 209.8853 25% ) PROBABLY 50%");

            // company info table
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Google, " +
                                     "PROBABLY Technology 50% / Internet Information Providers 50%, " +
                                     "PROBABLY London 33%/ Shanghai 33%/ Mountain View 34%)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Apple," +
                                     "PROBABLY Technology 50% / Personal Computer 50%, " +
                                     "PROBABLY Cupertino 33% / Hong Kong 33% / Santa Clare Valley 34%)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Microsoft, " +
                                     "Technology, unknown) PROBABLY 100%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Oracle, " +
                                     "Technology , SanFrancisco ) PROBABLY 50%");

            // regulator
            _analyticEngine.submitNonQuerySQL("INSERT INTO Regulator VALUES (Technology,London,Parliament)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO Regulator VALUES (Technology,Santa Clare Valley,Federal Electronics)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO Regulator VALUES (Internet Information Providers,London,Google)");

        }

        private void DropFinancialWorld()
        {
            _underlineDatabase.DropTableIfExist("CompanyInfo_0");
            _underlineDatabase.DropTableIfExist("CompanyInfo_1");
            _underlineDatabase.DropTableIfExist("CompanyInfo_2");
            _underlineDatabase.DropTableIfExist("CompanyInfo_3");
            _underlineDatabase.DropTableIfExist("CompanyInfo_PossibleStates");
            _underlineDatabase.DropTableIfExist("CompanyInfo_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("CompanyInfo_PossibleWorldsAggregated");

            _underlineDatabase.DropTableIfExist("CompanyStock_0");
            _underlineDatabase.DropTableIfExist("CompanyStock_1");
            _underlineDatabase.DropTableIfExist("CompanyStock_2");
            _underlineDatabase.DropTableIfExist("CompanyStock_PossibleStates");
            _underlineDatabase.DropTableIfExist("CompanyStock_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("CompanyStock_PossibleWorldsAggregated");

            _underlineDatabase.DropTableIfExist("Regulator_0");
            _underlineDatabase.DropTableIfExist("Regulator_1");
            _underlineDatabase.DropTableIfExist("Regulator_2");
            _underlineDatabase.DropTableIfExist("Regulator_3");
            _underlineDatabase.DropTableIfExist("Regulator_PossibleStates");
            _underlineDatabase.DropTableIfExist("Regulator_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("Regulator_PossibleWorldsAggregated");
        }
    }
}
