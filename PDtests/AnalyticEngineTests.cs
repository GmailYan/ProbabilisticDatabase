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
        readonly AnalyticEngine _analyticEngine = new AnalyticEngine();
        readonly StandardDatabase _underlineDatabase = new StandardDatabase();


        //[TestMethod]
        /// <summary>
        /// couldn't pass the possibleWorlds part yet, code still assume att1 as column name
        /// </summary>
        public void TesNormalInsertWithCreatedTable()
        {
            DropCarWorld();
            const string sentences = "CREATE TABLE Car (RegId int, Colour varchar(255), Mileage float)";

            _analyticEngine.submitNonQuerySQL(sentences);

            const string sentences2 = "INSERT INTO Car VALUES (101,red,500.25)";

            _analyticEngine.submitNonQuerySQL(sentences2);
        }

        private void DropCarWorld()
        {
            _underlineDatabase.DropTableIfExist("Car_Answer");
            _underlineDatabase.DropTableIfExist("Car_0");
            _underlineDatabase.DropTableIfExist("Car_1");
            _underlineDatabase.DropTableIfExist("Car_2");
            _underlineDatabase.DropTableIfExist("Car_3");
            _underlineDatabase.DropTableIfExist("Car_PossibleStates");
            _underlineDatabase.DropTableIfExist("Car_PossibleWorlds");
        }

        [TestMethod]
        public void EvaluateSingleInsertSQL()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            var table =_analyticEngine.viewTable("socialData_PossibleWorlds");
            Assert.AreEqual(table.Rows.Count,2);

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateDoubleInsertSQL()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353, Probably 186 10% / 786 90% ,Probably James 20%/ Jane 80%) PROBABLY 40%");
            var table = _analyticEngine.viewTable("socialData_PossibleWorlds");
            // 2 state 4 state, 8 worlds that each with 2 rows(random variables)
            Assert.AreEqual(table.Rows.Count, 2*4*2);

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateSimpleSelectSQL()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            DataTable table2;

            _analyticEngine.submitQuerySQL("SELECT att2 FROM socialData",out table2);

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
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 186 10% / 786 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;

            _analyticEngine.submitQuerySQL("SELECT att2 FROM socialData", out table2);
            Assert.IsNotNull(table2);
            Assert.AreEqual(table2.Rows.Count, 4);
            
            Assert.IsTrue(dataRowEqual(table2.Rows[0], "786", 90));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "785", 50));
            Assert.IsTrue(dataRowEqual(table2.Rows[2], "185", 50));
            Assert.IsTrue(dataRowEqual(table2.Rows[3], "186", 10));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateNormalSelectTupleMayNotExist()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 186 10% / 786 90% ,Probably James 20%/ Jane 80%) PROBABLY 40%");

            DataTable table2;

            _analyticEngine.submitQuerySQL("SELECT att2 FROM socialData WHERE (att1=353)", out table2);
            Assert.IsNotNull(table2);
            Assert.AreEqual(table2.Rows.Count, 2);

            Assert.IsTrue(dataRowEqual(table2.Rows[0], "786", 36));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "186", 4));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateSimpleSelectWhereClause()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 185 10% / 785 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;
            _analyticEngine.submitQuerySQL("SELECT att2 FROM socialData WHERE (att3 = 'SMITH')", out table2);
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

        private bool dataRowEqual2(DataRow dataRow, params string[] values)
        {
            int rowSize = dataRow.Table.Columns.Count;
            if ( rowSize != values.Length)
            {
                return false;
            }

            for (int i = 0; i < rowSize; i++)
            {
                var stringRowValue = dataRow[i].ToString();
                if (stringRowValue != values[i])
                {
                    return false;
                }
            }

            return true;
        }

        [TestMethod]
        public void EvaluateSimpleSelectWhereClause2()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 185 10% / 785 90% ,Probably James 20%/ Jane 80%)");

            DataTable table2;
            _analyticEngine.submitQuerySQL("SELECT att2 FROM socialData", out table2);
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
            _analyticEngine.submitQuerySQL("select att3,att2,t2att1,t2att2 from "+
                "[socialData as t1 join (SELECT att1 as t2att1,att2 as t2att2 FROM socialInfo) as t2 on att2=t2att1] where (att3='SMITH')",out table);
            DropBiggerSocialDataWorld();

            Assert.IsNotNull(table);
            Assert.IsTrue(table.Rows.Count==4);
            Assert.IsTrue(dataRowEqual2(table.Rows[0], "SMITH", "785", "785", "LowIncome", "45"));
            Assert.IsTrue(dataRowEqual2(table.Rows[1], "SMITH", "185", "185", "HighIncome", "25"));
            Assert.IsTrue(dataRowEqual2(table.Rows[2], "SMITH", "185","185" ,"NormalIncome", "25"));
            Assert.IsTrue(dataRowEqual2(table.Rows[3], "SMITH", "785","785", "NormalIncome", "5"));
            
        }


        private void SetupBiggerSocialDataWorld()
        {
            //clean the environment before start actual test
            DropBiggerSocialDataWorld();

            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably 185 10% / 785 90% ,Probably James 20%/ Jane 80%)");

            _analyticEngine.submitNonQuerySQL("INSERT INTO socialInfo VALUES (185,PROBABLY HighIncome 50% / NormalIncome 50%,NoSupportNeeded)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialInfo VALUES (785,Probably NormalIncome 10% / LowIncome 90% ,Probably NeedSupport 20%/ NoSupportNeeded 80%)");
 

        }

        private void DropBiggerSocialDataWorld()
        {
            DropSocialDataTables();

            _underlineDatabase.DropTableIfExist("socialInfo_Temp");
            _underlineDatabase.DropTableIfExist("socialInfo_Answer");
            _underlineDatabase.DropTableIfExist("socialInfo_0");
            _underlineDatabase.DropTableIfExist("socialInfo_1");
            _underlineDatabase.DropTableIfExist("socialInfo_2");
            _underlineDatabase.DropTableIfExist("socialInfo_3");
            _underlineDatabase.DropTableIfExist("socialInfo_PossibleStates");
            _underlineDatabase.DropTableIfExist("socialInfo_PossibleWorlds");
        }

        public void GetAllGoogleStockPrice(){
            SetupFinancialWorld();

            DataTable result;
            _analyticEngine.submitQuerySQL("SELECT att0,att1 FROM CompanyStock WHERE att0 = 'Google' EVALUATE USing monte carlo", out result);

            // Google has 3 possible stock price
            Assert.AreEqual(result.Rows.Count, 3);

            DropFinancialWorld();
        }

        private void SetupFinancialWorld()
        {
            //clean the environment before start actual test
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
                                     "PROBABLY Cupertino 50% / Hong Kong 50% / Santa Clare Valley 50%)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Microsoft, " +
                                     "Technology, unknown) PROBABLY 100%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO CompanyInfo VALUES (Oracle, " +
                                     " , San Francisco ) PROBABLY 50%");

        }

        private void DropFinancialWorld()
        {
            _underlineDatabase.DropTableIfExist("CompanyInfo_0");
            _underlineDatabase.DropTableIfExist("CompanyInfo_1");
            _underlineDatabase.DropTableIfExist("CompanyInfo_2");
            _underlineDatabase.DropTableIfExist("CompanyInfo_3");
            _underlineDatabase.DropTableIfExist("CompanyInfo_PossibleStates");
            _underlineDatabase.DropTableIfExist("CompanyInfo_PossibleWorlds");

            _underlineDatabase.DropTableIfExist("CompanyStock_0");
            _underlineDatabase.DropTableIfExist("CompanyStock_1");
            _underlineDatabase.DropTableIfExist("CompanyStock_2");
            _underlineDatabase.DropTableIfExist("CompanyStock_PossibleStates");
            _underlineDatabase.DropTableIfExist("CompanyStock_PossibleWorlds");
        }

        private void DropSocialDataTables()
        {
            _underlineDatabase.DropTableIfExist("socialData_Temp");
            _underlineDatabase.DropTableIfExist("socialData_Answer");
            _underlineDatabase.DropTableIfExist("socialData_0");
            _underlineDatabase.DropTableIfExist("socialData_1");
            _underlineDatabase.DropTableIfExist("socialData_2");
            _underlineDatabase.DropTableIfExist("socialData_3");
            _underlineDatabase.DropTableIfExist("socialData_PossibleStates");
            _underlineDatabase.DropTableIfExist("socialData_PossibleWorlds");
        }
    }
}
