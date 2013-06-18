using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProbabilisticDatabase.Src.ControllerPackage;
using ProbabilisticDatabase.Src.ControllerPackage.QueryHandler;
using ProbabilisticDatabase.Src.DatabaseEngine;

namespace PDtests
{
    [TestClass]
    public class test
    {
        readonly AnalyticEngine _analyticEngine = new AnalyticEngine();
        readonly StandardDatabase _underlineDatabase = new StandardDatabase();

        [TestMethod]
        public void EvaluateSMALLLazy()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverSmallWorld();
            
            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            string sql2 = "Select t1.att1,t2.att2 from Withness as t1 join Car as t2 on t1.att2=t2.att3 and t1.att3=t2.att4";
            string sql1 = "Select att2 From Crime";
            _analyticEngine.submitQuerySQL(sql2, out rTable);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds/10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);

        }

        [TestMethod]
        public void EvaluateSMALLNaive()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverSmallWorld();

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            string sql1 = "Select att2 From Crime Evaluate Using (Naive)";

            var subselect = "Select att2 as t2att2,att3 as t2att3, att4 as t2att4 from Car";
            string sql = "SELECT att1,t2att2 FROM [Withness as t1 JOIN (" + subselect + ") as t2 ON t1.att2=t2att3 AND t1.att3=t2att4]";
            string sql2 = sql+" Evaluate Using (Naive)";
            _analyticEngine.submitQuerySQL(sql2, out rTable);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);

        }

        [TestMethod]
        public void EvaluateSMALLExtensional()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverSmallWorld();

            string s = "Select t.att2,100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p from (Select t.att2,Sum(t.p) as p from Crime_PossibleStates as t group by t.var,t.att2) as t group by t.att2";
            string s2 = "Select R1.att1,R2.att2,R1.P*R2.P as P " +
                        "from (Select t.att1,t.att2,t.att3, 100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p " +
                        "from (Select t.att1,t.att2,t.att3,Sum(t.p) as p from Withness_PossibleStates as t group by t.var,t.att1,t.att2,t.att3) as t group by t.att1,t.att2,t.att3) as R1 " +
                        "join (Select t.att2,t.att3,t.att4, 100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p " +
                        "from (Select t.att2,t.att3,t.att4,Sum(t.p) as p from Car_PossibleStates as t group by t.var,t.att2,t.att3,t.att4) as t group by t.att2,t.att3,t.att4) as R2 " +
                        "on R1.att2=R2.att3 and R1.att3=R2.att4";
            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            rTable = _underlineDatabase.ExecuteSqlWithResult(s2);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);
            
        }

        [TestMethod]
        public void EvaluateMedLazy()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverMediumWorld();

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            string sql2 = "Select t1.att1,t2.att2 from Withness as t1 join Car as t2 on t1.att2=t2.att3 and t1.att3=t2.att4";
            string sql1 = "Select att2 From Crime";
            _analyticEngine.submitQuerySQL(sql2, out rTable);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);

        }

        [TestMethod]
        public void EvaluateMedNaive()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverMediumWorld();

            var subselect = "Select att2 as t2att2,att3 as t2att3, att4 as t2att4 from Car";
            string sql = "SELECT att1,t2att2 FROM [Withness as t1 JOIN (" + subselect + ") as t2 ON t1.att2=t2att3 AND t1.att3=t2att4]";
            string sql2 = sql + " Evaluate Using (Naive)";

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            _analyticEngine.submitQuerySQL(sql2, out rTable);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);

        }

        [TestMethod]
        public void EvaluateMedExtensional()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverMediumWorld();

            string s = "Select t.att2,100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p from (Select t.att2,Sum(t.p) as p from Crime_PossibleStates as t group by t.var,t.att2) as t group by t.att2";
            string s2 = "Select R1.att1,R2.att2,R1.P*R2.P as P " +
             "from (Select t.att1,t.att2,t.att3, 100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p " +
             "from (Select t.att1,t.att2,t.att3,Sum(t.p) as p from Withness_PossibleStates as t group by t.var,t.att1,t.att2,t.att3) as t group by t.att1,t.att2,t.att3) as R1 " +
             "join (Select t.att2,t.att3,t.att4, 100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p " +
             "from (Select t.att2,t.att3,t.att4,Sum(t.p) as p from Car_PossibleStates as t group by t.var,t.att2,t.att3,t.att4) as t group by t.att2,t.att3,t.att4) as R2 " +
             "on R1.att2=R2.att3 and R1.att3=R2.att4";

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            rTable = _underlineDatabase.ExecuteSqlWithResult(s2);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);
        }

        [TestMethod]
        public void EvaluateBigLazy()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverBigWorld();

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            string sql2 = "Select t1.att1,t2.att2 from Withness as t1 join Car as t2 on t1.att2=t2.att3 and t1.att3=t2.att4";
            string sql1 = "Select att2 From Crime";
            _analyticEngine.submitQuerySQL(sql2, out rTable);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);

        }

        [TestMethod]
        public void EvaluateBigNaive()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverBigWorld();

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            _analyticEngine.submitQuerySQL("Select att2 From Crime Evaluate Using (Naive)", out rTable);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);

        }

        [TestMethod]
        public void EvaluateBigExtensional()
        {
            Stopwatch sw = new Stopwatch();
            TimeSpan ts = sw.Elapsed;

            DropSocialDataTables();
            SetUpCrimeSolverBigWorld();

            string s = "Select t.att2,100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p from (Select t.att2,Sum(t.p) as p from Crime_PossibleStates as t group by t.var,t.att2) as t group by t.att2";
            string s2 = "Select R1.att1,R2.att2,R1.P*R2.P as P " +
  "from (Select t.att1,t.att2,t.att3, 100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p " +
  "from (Select t.att1,t.att2,t.att3,Sum(t.p) as p from Withness_PossibleStates as t group by t.var,t.att1,t.att2,t.att3) as t group by t.att1,t.att2,t.att3) as R1 " +
  "join (Select t.att2,t.att3,t.att4, 100-Exp(Sum(Log(100-(case when t.p>=100 then 99.99999999 else t.p end)))) as p " +
  "from (Select t.att2,t.att3,t.att4,Sum(t.p) as p from Car_PossibleStates as t group by t.var,t.att2,t.att3,t.att4) as t group by t.att2,t.att3,t.att4) as R2 " +
  "on R1.att2=R2.att3 and R1.att3=R2.att4";

            sw = new Stopwatch();
            sw.Start();
            DataTable rTable;
            rTable = _underlineDatabase.ExecuteSqlWithResult(s2);
            sw.Stop();
            ts = sw.Elapsed;
            var elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Reconstruction RunTime " + elapsedTime);
        }

        private void PreparePossibleTables(string tName)
        {
            SelectQueryHandler queryHandler = new SelectQueryHandler(null, _underlineDatabase);
            var tables = new List<string>();
            tables.Add(tName);
            queryHandler.PrepareReleventTables(tables);
        }

        [TestMethod]
        public void DropSocialDataTables()
        {
            _underlineDatabase.DropTableIfExist("Crime_0");
            _underlineDatabase.DropTableIfExist("Crime_1");
            _underlineDatabase.DropTableIfExist("Crime_2");
            _underlineDatabase.DropTableIfExist("Crime_Answer");
            _underlineDatabase.DropTableIfExist("Crime_PossibleStates");
            _underlineDatabase.DropTableIfExist("Crime_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("Crime_PossibleWorldsAggregated");

            _underlineDatabase.DropTableIfExist("Withness");
            _underlineDatabase.DropTableIfExist("Withness_0");
            _underlineDatabase.DropTableIfExist("Withness_1");
            _underlineDatabase.DropTableIfExist("Withness_2");
            _underlineDatabase.DropTableIfExist("Withness_23");
            _underlineDatabase.DropTableIfExist("Withness_3");
            _underlineDatabase.DropTableIfExist("Withness_PossibleStates");
            _underlineDatabase.DropTableIfExist("Withness_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("Withness_PossibleWorldsAggregated");

            _underlineDatabase.DropTableIfExist("Car");
            _underlineDatabase.DropTableIfExist("Car_0");
            _underlineDatabase.DropTableIfExist("Car_1");
            _underlineDatabase.DropTableIfExist("Car_2");
            _underlineDatabase.DropTableIfExist("Car_3");
            _underlineDatabase.DropTableIfExist("Car_4");
            _underlineDatabase.DropTableIfExist("Car_PossibleStates");
            _underlineDatabase.DropTableIfExist("Car_PossibleWorlds");
            _underlineDatabase.DropTableIfExist("Car_PossibleWorldsAggregated");

            _underlineDatabase.DropTableIfExist("answer");
            _underlineDatabase.DropTableIfExist("OrderedSetOfWorldNo");

        }
        private void SetUpCrimeSolverSmallWorld()
        {
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Bank,Yes)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store,No)");

            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK2,Blue)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Toyota MK1,Green)");


            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1500,Tom,Toyota MK2,Probably Blue 50% / Black 50%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (3500,Jerry,Toyota MK1,Black)");

        }

        private void SetUpCrimeSolverMediumWorld()
        {
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Bank,Yes)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Resturant,Yes)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store 2,Probably No 90% / Yes 10%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store 2,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Mall, Probably No 45% / Yes 55%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (1st Avnue,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Probably 1st Avnue 10% / 2nd Avnue 90%,No)");


            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK2,Blue)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Probably [Honda Red] 70% / [Honda Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jerry,Probably [ToyotaFury Red] 70% / [ToyotaFury Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jeany,ToyotaFury,Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jerry,Probably [Honda Red] 70% / [Honda Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jeany,Honda Civic,Purple)");

            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1500,Tom,Toyota MK2,Probably Blue 50% / Black 50%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (3500,Jerry,Toyota MK1,Black)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (2000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (2500,Probably William 50% / Winston 50%,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3567 70% / 4399 30%,James the second,Honda, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3567 70% / 4399 30%,Probably James 25% / Jane 75%,ToyotaFury, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3533 40% / 4399 60%,Probably William 50% / Winston 50%,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (4200,Tony,Honda Civic, Probably Red 70% / Purple 30%)");
        }

        private void SetUpCrimeSolverBigWorld()
        {
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Bank,Yes)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Resturant,Yes)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store 2,Probably No 90% / Yes 10%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store 2,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Mall, Probably No 45% / Yes 55%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (1st Avnue,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Probably 1st Avnue 10% / 2nd Avnue 90%,No)");
            _analyticEngine.submitNonQuerySQL(
                 "INSERT INTO Crime VALUES (Jewelry Store 2,Probably No 90% / Yes 10%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store 2,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Mall, Probably No 45% / Yes 55%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (1st Avnue,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Probably 1st Avnue 10% / 2nd Avnue 90%,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store,Probably No 10% / Yes 90%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Watch Store,No)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Resturant,Yes)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Crime VALUES (Jewelry Store 2,Probably No 90% / Yes 10%)");



            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK2,Blue)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Probably [Honda Red] 70% / [Honda Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jerry,Probably [ToyotaFury Red] 70% / [ToyotaFury Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jeany,ToyotaFury,Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jerry,Probably [Honda Red] 70% / [Honda Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Jeany,Honda Civic,Purple)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK2,Blue)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tom,Toyota MK1, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (Tony,Probably [Honda Red] 70% / [Honda Purple] 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Withness VALUES (William,Toyota MK1,Green)");


            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1500,Tom,Toyota MK2,Probably Blue 50% / Black 50%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (3500,Jerry,Toyota MK1,Black)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (2000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (2500,Probably William 50% / Winston 50%,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3567 70% / 4399 30%,James the second,Honda, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3567 70% / 4399 30%,Probably James 25% / Jane 75%,ToyotaFury, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3533 40% / 4399 60%,Probably William 50% / Winston 50%,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (4200,Tony,Honda Civic, Probably Red 70% / Purple 30%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (1500,Tom,Toyota MK2,Probably Blue 50% / Black 50%)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (3500,Jerry,Toyota MK1,Black)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (2000,Probably James 25% / Jane 75%,Toyota MK1, Red)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (2500,Probably William 50% / Winston 50%,Toyota MK1,Green)");
            _analyticEngine.submitNonQuerySQL(
                "INSERT INTO Car VALUES (Probably 3567 70% / 4399 30%,James the second,Honda, Probably Red 70% / Purple 30%)");


        }

    }
}
