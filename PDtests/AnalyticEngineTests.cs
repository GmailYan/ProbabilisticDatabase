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
            var table0 =_analyticEngine.viewTable("socialData_0");
            Assert.IsTrue(table0.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table0.Rows[0], "1", "1", "socialData_1,socialData_2,socialData_3", "50"));

            var table1 = _analyticEngine.viewTable("socialData_1");
            Assert.IsTrue(table1.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table1.Rows[0],"1","1","351","100"));

            var table2 = _analyticEngine.viewTable("socialData_2");
            Assert.IsTrue(table2.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table2.Rows[0], "1", "1", "785", "50"));
            Assert.IsTrue(dataRowEqual2(table2.Rows[1], "1", "2", "185", "50"));

            var table3 = _analyticEngine.viewTable("socialData_3");
            Assert.IsTrue(table3.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table3.Rows[0], "1", "1", "SMITH", "100"));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateDoubleInsertSQL()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY 785 50% / 185 50% ,SMITH) PROBABLY 50%");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (353,Probably [186 James] 10% / [786 Jane] 90%) PROBABLY 40%");
            
            var table0 = _analyticEngine.viewTable("socialData_0");
            Assert.IsTrue(table0.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table0.Rows[0], "1", "1", "socialData_1,socialData_2,socialData_3", "50"));
            Assert.IsTrue(dataRowEqual2(table0.Rows[1], "2", "1", "socialData_1,socialData_23", "40"));

            var table1 = _analyticEngine.viewTable("socialData_1");
            Assert.IsTrue(table1.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table1.Rows[0], "1", "1", "351", "100"));
            Assert.IsTrue(dataRowEqual2(table1.Rows[1], "2", "1", "353", "100"));

            var table2 = _analyticEngine.viewTable("socialData_2");
            Assert.IsTrue(table2.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table2.Rows[0], "1", "1", "785", "50"));
            Assert.IsTrue(dataRowEqual2(table2.Rows[1], "1", "2", "185", "50"));

            var table3 = _analyticEngine.viewTable("socialData_3");
            Assert.IsTrue(table3.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table3.Rows[0], "1", "1", "SMITH", "100"));

            var table23 = _analyticEngine.viewTable("socialData_23");
            Assert.IsTrue(table23.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table23.Rows[0], "2", "1", "186", "James", "10"));
            Assert.IsTrue(dataRowEqual2(table23.Rows[1], "2", "2", "786", "Jane", "90"));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateInsertPartlyDependent()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData VALUES (351,PROBABLY [785 SMITH] 50% / [185 James] 50%)");
            
            var table0 = _analyticEngine.viewTable("socialData_0");
            Assert.IsTrue(table0.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table0.Rows[0], "1", "1", "socialData_1,socialData_23", "100"));

            var table1 = _analyticEngine.viewTable("socialData_1");
            Assert.IsTrue(table1.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table1.Rows[0], "1", "1", "351", "100"));

            var table2 = _analyticEngine.viewTable("socialData_23");
            Assert.IsTrue(table2.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table2.Rows[0], "1", "1", "785","SMITH" ,"50"));
            Assert.IsTrue(dataRowEqual2(table2.Rows[1], "1", "2", "185","James" ,"50"));

            DropSocialDataTables();
        }


        [TestMethod]
        public void EvaluateInsertInReverseOrder()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData(att4,att1,att3,att2) VALUES (Single,351,PROBABLY [SMITH 785] 40% / [James 185] 60%)");
            
            var table0 = _analyticEngine.viewTable("socialData_0");
            Assert.IsTrue(table0.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table0.Rows[0], "1", "1", "socialData_4,socialData_1,socialData_32", "100"));

            var table1 = _analyticEngine.viewTable("socialData_1");
            Assert.IsTrue(table1.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table1.Rows[0], "1", "1", "351", "100"));

            var table2 = _analyticEngine.viewTable("socialData_32");
            Assert.IsTrue(table2.Rows.Count == 2);
            Assert.IsTrue(dataRowEqual2(table2.Rows[0], "1", "1", "SMITH", "785", "40"));
            Assert.IsTrue(dataRowEqual2(table2.Rows[1], "1", "2", "James", "185", "60"));

            var table4 = _analyticEngine.viewTable("socialData_4");
            Assert.IsTrue(table4.Rows.Count == 1);
            Assert.IsTrue(dataRowEqual2(table4.Rows[0], "1", "1", "Single", "100"));

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
            Assert.AreEqual(table2.Rows.Count, 3);

            Assert.IsTrue(dataRowEqual(table2.Rows[0], "", 50));
            Assert.IsTrue(dataRowEqual(table2.Rows[1], "185", 25));
            Assert.IsTrue(dataRowEqual(table2.Rows[2], "785", 25));

            DropSocialDataTables();
        }

        [TestMethod]
        public void EvaluateSelectWithTuplesHasDependency()
        {
            DropSocialDataTables();
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData(att1,att3,att4,att2) VALUES (351,PROBABLY [785 SMITH] 50% / [185 James] 50%, Single)");
            _analyticEngine.submitNonQuerySQL("INSERT INTO socialData(att1,att2,att3,att4) VALUES (351,Married,PROBABLY [786 Jane] 100% )"); 
            DataTable table2;

            _analyticEngine.submitQuerySQL("SELECT att3,att4 FROM socialData WHERE (att3 > 200)", out table2);

            Assert.IsNotNull(table2);
            Assert.AreEqual(table2.Rows.Count, 2);

            Assert.IsTrue(dataRowEqual2(table2.Rows[0], "786","Jane", "100"));
            Assert.IsTrue(dataRowEqual2(table2.Rows[1], "785","SMITH", "50"));

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
        public void EvaluateSelectTuplesSameValue()
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
            _underlineDatabase.DropTableIfExist("socialInfo_PossibleWorldsAggregated");
        }

        [TestMethod]
        public void GetAllGoogleStockPrice(){
            SetupFinancialWorld();

            DataTable result;
            _analyticEngine.submitQuerySQL("SELECT att1,att2 FROM CompanyStock WHERE (att1 = 'Google')", out result);

            // Google has 3 possible stock price
            Assert.AreEqual(result.Rows.Count, 3);

            DropFinancialWorld();
        }

        [TestMethod]
        public void GetHighStockPriceCompany()
        {
            SetupFinancialWorld();

            DataTable result;
            _analyticEngine.submitQuerySQL("SELECT att1 FROM CompanyStock WHERE (att2 > 458.0)", out result);

            // Google has 3 possible stock price
            Assert.AreEqual(result.Rows.Count, 2);
            Assert.IsTrue(dataRowEqual(result.Rows[0],"Google",100));
            Assert.IsTrue(dataRowEqual(result.Rows[1],"Apple", 75));

            DropFinancialWorld();
        }

        [TestMethod]
        public void GetCompanyRegulator()
        {
            SetupFinancialWorld();

            DataTable result;
            string table3 = string.Format("SELECT att1,att2,att3 FROM Regulator");
            string table23SQL =
                string.Format(
                    "SELECT t1.att1 as t2att1,t2.att3 as t3att3 FROM [CompanyInfo as t1 JOIN ({0}) as t2 on t1.att2=t2.att2 AND t1.att1=t2.att1]",table3);
            string sql =
                string.Format("SELECT att1,t2.t3att3 FROM [CompanyStock as t1 JOIN ({0}) as t2 ON t1.att1=t2.t2att1]", table23SQL);
            _analyticEngine.submitQuerySQL(sql, out result);

            // Google has 3 possible stock price
            Assert.AreEqual(result.Rows.Count, 2);
            Assert.IsTrue(dataRowEqual(result.Rows[0], "Google", 100));
            Assert.IsTrue(dataRowEqual(result.Rows[1], "Apple", 75));

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
                                     "Technology , San Francisco ) PROBABLY 50%");

            // regulator
            _analyticEngine.submitNonQuerySQL("INSERT INTO Regulator VALUES (Technology,London,Parliament)");
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

        private void DropSocialDataTables()
        {
            _underlineDatabase.DropTableIfExist("socialData_Temp");
            _underlineDatabase.DropTableIfExist("socialData_Answer");
            _underlineDatabase.DropTableIfExist("socialData_0");
            _underlineDatabase.DropTableIfExist("socialData_1");
            _underlineDatabase.DropTableIfExist("socialData_2");
            _underlineDatabase.DropTableIfExist("socialData_23");
            _underlineDatabase.DropTableIfExist("socialData_3");
            _underlineDatabase.DropTableIfExist("socialData_34");
            _underlineDatabase.DropTableIfExist("socialData_4");
            _underlineDatabase.DropTableIfExist("socialData_32");
            _underlineDatabase.DropTableIfExist("socialData_PossibleStates");
            _underlineDatabase.DropTableIfExist("socialData_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("socialData_PossibleWorldsAggregated");
        }
    }
}
