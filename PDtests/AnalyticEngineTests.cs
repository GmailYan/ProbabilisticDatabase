using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProbabilisticDatabase.Src.ControllerPackage;
using ProbabilisticDatabase.Src.DatabaseEngine;

namespace PDtests
{
    [TestClass]
    public class AnalyticEngineTests
    {
        AnalyticEngine analyticEngine = new AnalyticEngine();
        StandardDatabase underlineDatabase = new StandardDatabase();


        [TestMethod]
        public void TesNormalInsertWithCreatedTable()
        {
            DropCarWorld();
            string sentences = "CREATE TABLE Car (RegId int, Colour varchar(255), Mileage float)";

            analyticEngine.submitNonQuerySQL(sentences);

            string sentences2 = "INSERT INTO Car VALUES (101,red,500.25)";

            analyticEngine.submitNonQuerySQL(sentences2);
        }

        private void DropCarWorld()
        {
            underlineDatabase.DropTableIfExist("Car_Answer");
            underlineDatabase.DropTableIfExist("Car_0");
            underlineDatabase.DropTableIfExist("Car_1");
            underlineDatabase.DropTableIfExist("Car_2");
            underlineDatabase.DropTableIfExist("Car_PossibleStates");
            underlineDatabase.DropTableIfExist("Car_PossibleWorlds");
        }

        [TestMethod]
        public void EvaluateSingleInsertSQL()
        {
            DropSocialDataTables();
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            var table =analyticEngine.viewTable("socialData_PossibleWorlds");
            Assert.AreEqual(table.Rows.Count,2);

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateDoubleInsertSQL()
        {
            DropSocialDataTables();
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353, Probably 186 10% / 786 90% ,Probably James 20%/ Jane 80%) PROBABLY 40%");
            var table = analyticEngine.viewTable("socialData_PossibleWorlds");
            // 2 state 4 state, 8 worlds that each with 2 rows(random variables)
            Assert.AreEqual(table.Rows.Count, 2*4*2);

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateSimpleSelectSQL()
        {
            DropSocialDataTables();
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            DataTable table2;

            analyticEngine.submitQuerySQL("SELECT att2 FROM socialData",out table2);

            Assert.IsNotNull(table2);
            Assert.AreEqual(table2.Rows.Count, 2);

            Assert.IsTrue(dataRowEqual(table2.Rows[0], "185", 25));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "785", 25));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateNormalSelectSQL()
        {
            DropSocialDataTables();
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 186 10% / 786 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;

            analyticEngine.submitQuerySQL("SELECT att2 FROM socialData", out table2);
            Assert.IsNotNull(table2);
            Assert.AreEqual(table2.Rows.Count, 4);
            
            Assert.IsTrue(dataRowEqual(table2.Rows[0], "786", 90));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "785", 50));
            Assert.IsTrue(dataRowEqual(table2.Rows[2], "185", 50));
            Assert.IsTrue(dataRowEqual(table2.Rows[3], "186", 10));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateSimpleSelectWhereClause()
        {
            DropSocialDataTables();
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 185 10% / 785 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;
            analyticEngine.submitQuerySQL("SELECT att2 FROM socialData WHERE (att3 = 'SMITH')", out table2);
            // correct probability distribution would be 785 50% / 185 50%

            Assert.IsTrue(table2.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual(table2.Rows[0], "185", 50));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "785", 50));
            DropSocialDataTables();
        }

        private bool dataRowEqual(DataRow dataRow, string field1, int field2)
        {
            if (dataRow[0].ToString() != field1)
            {
                return false;
            }

            int integer;
            int.TryParse(dataRow[1].ToString(), out integer);

            if (integer != field2)
            {
                return false;
            }

            return true;
        }

        [TestMethod]
        public void EvaluateSimpleSelectWhereClause2()
        {
            DropSocialDataTables();
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 185 10% / 785 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;
            analyticEngine.submitQuerySQL("SELECT att2 FROM socialData", out table2);
            Assert.IsTrue(table2.Rows.Count == 2);
            // correct probability distribution would be 185 55%, 785 95%
            Assert.IsTrue(dataRowEqual(table2.Rows[0], "785", 95));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "185", 55));
            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateSelectQueryWithJoin()
        {
            SetupBiggerSocialDataWorld();

            DataTable table;
            analyticEngine.submitQuerySQL("select att2 from [socialData as t1 join (SELECT * FROM socialInfo) as t2 on t1.att2=t2.att1] where (t1.att3='SMITH')",out table);

            DropBiggerSocialDataWorld();
        }


        private void SetupBiggerSocialDataWorld()
        {
            //clean the environment before start actual test
            DropBiggerSocialDataWorld();

            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 185 10% / 785 90% ,Probably James 20%/ Jane 80%)");

            analyticEngine.submitNonQuerySQL("INSERT INTO socialInfo VALUES (185,PROBABLY HighIncome 50% / NormalIncome 50%,NoSupportNeeded)");
            analyticEngine.submitNonQuerySQL("INSERT INTO socialInfo VALUES (785,Probably NormalIncome 10% / LowIncome 90% ,Probably NeedSupport 20%/ NoSupportNeeded 80%)");
 

        }

        private void DropBiggerSocialDataWorld()
        {
            DropSocialDataTables();

            underlineDatabase.DropTableIfExist("socialInfo_Temp");
            underlineDatabase.DropTableIfExist("socialInfo_Answer");
            underlineDatabase.DropTableIfExist("socialInfo_0");
            underlineDatabase.DropTableIfExist("socialInfo_1");
            underlineDatabase.DropTableIfExist("socialInfo_2");
            underlineDatabase.DropTableIfExist("socialInfo_3");
            underlineDatabase.DropTableIfExist("socialInfo_PossibleStates");
            underlineDatabase.DropTableIfExist("socialInfo_PossibleWorlds");
        }

        public void GetAllGoogleStockPrice(){
            SetupFinancialWorld();

            DataTable result;
            analyticEngine.submitQuerySQL("SELECT att0,att1 FROM CompanyStock WHERE att0 = 'Google' EVALUATE USing monte carlo", out result);

            // Google has 3 possible stock price
            Assert.AreEqual(result.Rows.Count, 3);

            DropFinancialWorld();
        }

        private void SetupFinancialWorld()
        {
            //clean the environment before start actual test
            DropFinancialWorld();

            //stock table
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (Google, " +
                                     "PROBABLY 920.1103333841 25% / 839.8293413259 50% / 766.5529849641 25% )");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (Apple," +
                                     "Probably 462.4615296 25% / 458.1030132 50% / 453.7855740875 25% )");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (Microsoft, " +
                                     "Probably 28.1324924976 25% / 28.1099955024 50% / 28.0875164976 25% ) PROBABLY 100%");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (IBM_1, " +
                                     "Probably 258.2503 25% / 223.03435 50% / 192.620575 25% ) PROBABLY 50%");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyStock VALUES (IBM_2, " +
                                     "Probably 212.25447632 25% / 211.066564 50% / 209.8853 25% ) PROBABLY 50%");

            // company info table
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Google, " +
                                     "PROBABLY Technology 50% / Internet Information Providers 50%, " +
                                     "PROBABLY London 33%/ Shanghai 33%/ Mountain View 34%)");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Apple," +
                                     "PROBABLY Technology 50% / Personal Computer 50%, " +
                                     "PROBABLY Cupertino 50% / Hong Kong 50% / Santa Clare Valley 50%)");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Microsoft, " +
                                     "Technology, unknown) PROBABLY 100%");
            analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Oracle, " +
                                     " , San Francisco ) PROBABLY 50%");

        }

        private void DropFinancialWorld()
        {
            underlineDatabase.DropTableIfExist("CompanyInfo_0");
            underlineDatabase.DropTableIfExist("CompanyInfo_1");
            underlineDatabase.DropTableIfExist("CompanyInfo_2");
            underlineDatabase.DropTableIfExist("CompanyInfo_3");
            underlineDatabase.DropTableIfExist("CompanyInfo_PossibleStates");
            underlineDatabase.DropTableIfExist("CompanyInfo_PossibleWorlds");

            underlineDatabase.DropTableIfExist("CompanyStock_0");
            underlineDatabase.DropTableIfExist("CompanyStock_1");
            underlineDatabase.DropTableIfExist("CompanyStock_2");
            underlineDatabase.DropTableIfExist("CompanyStock_PossibleStates");
            underlineDatabase.DropTableIfExist("CompanyStock_PossibleWorlds");
        }

        private void DropSocialDataTables()
        {
            underlineDatabase.DropTableIfExist("socialData_Temp");
            underlineDatabase.DropTableIfExist("socialData_Answer");
            underlineDatabase.DropTableIfExist("socialData_0");
            underlineDatabase.DropTableIfExist("socialData_1");
            underlineDatabase.DropTableIfExist("socialData_2");
            underlineDatabase.DropTableIfExist("socialData_3");
            underlineDatabase.DropTableIfExist("socialData_PossibleStates");
            underlineDatabase.DropTableIfExist("socialData_PossibleWorlds");
        }
    }
}
