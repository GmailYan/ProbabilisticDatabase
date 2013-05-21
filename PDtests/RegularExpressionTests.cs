using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.RegularExpressions;
using ProbabilisticDatabase.Src.ControllerPackage.Query;
using ProbabilisticDatabase.Src.ControllerPackage.Query.InsertQuery;
using ProbabilisticDatabase.Src.DatabaseEngine;
using ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery;

namespace PDtests
{
    [TestClass]
    public class RegularExpressionTests
    {
        [TestMethod]
        public void TestNormalSqlInsert()
        {
            string sentences = "INSERT INTO socialData VALUES (351,785,Smith,Single)";

            SqlInsertQuery query = new SqlInsertQuery(sentences);

            query.processAndPopulateEachField();
            
            Assert.IsTrue(query.TupleP == 100.0);
            Assert.IsTrue(query.TableName == "socialData");
            Assert.IsTrue(query.Attributes.Count == 4);
        
        }

        [TestMethod]
        public void TestProbabilisticSqlInsert()
        {
            string sentences = "INSERT INTO socialData VALUES (351,PROBABLY 785 25% ,Smith,Single) probably 50%";

            SqlInsertQuery query = new SqlInsertQuery(sentences);

            query.processAndPopulateEachField();

            Assert.IsTrue(query.TupleP == 50.0);
            Assert.IsTrue(query.TableName == "socialData");
            Assert.IsTrue(query.Attributes.Count == 4);

            ProbabilisticAttribute p = (ProbabilisticAttribute)query.Attributes[1];
            Assert.IsTrue(p.Values[0] == "785");
            Assert.IsTrue(p.Probs[0] == 25.0);
        }

        [TestMethod]
        public void TestComplexProbabilisticSqlInsert()
        {
            string sentences = "INSERT INTO socialData VALUES (351,PROBABLY 785 25%/ 185 70% ,Smith,Single) probably 50%";

            SqlInsertQuery query = new SqlInsertQuery(sentences);

            query.processAndPopulateEachField();

            Assert.IsTrue(query.TupleP == 50.0);
            Assert.IsTrue(query.TableName == "socialData");
            Assert.IsTrue(query.Attributes.Count == 4);

            ProbabilisticAttribute p = (ProbabilisticAttribute)query.Attributes[1];
            Assert.IsTrue(p.Values[0] == "785");
            Assert.IsTrue(p.Probs[0] == 25.0);
            Assert.IsTrue(p.Values[1] == "185");
            Assert.IsTrue(p.Probs[1] == 70.0);
        }

        [TestMethod]
        public void TestParsingProbClause()
        {
            string sentences = "785 25% / 185 50% ";

            ProbabilisticAttribute pa = SqlInsertQuery.processProbabilisticValueClause(sentences);

            Assert.IsTrue(pa.Probs.Count == 2);
            Assert.IsTrue(pa.Values.Count == 2);
            Assert.IsTrue(pa.Values[1] == "185");
            Assert.IsTrue(pa.Probs[1] == 50.0);
        }

        [TestMethod]
        public void TestParsingSelectQueryWithWhereClause()
        {
            string sentences = "Select a,b,c from Data where a=25 and b=15 ";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "a=25 and b=15");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.Default));
        }

        [TestMethod]
        public void TestParsingSelectQueryWithWhereClauseAndStrategy()
        {
            string sentences = "Select a,b,c from Data where a=25 and b=15 Evaluate using monte Carlo";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "a=25 and b=15");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.MonteCarlo));
        }

        [TestMethod]
        public void TestParsingSelectQueryWithOnlyStrategy()
        {
            string sentences = "Select a,b,c from Data Evaluate using monte Carlo";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.MonteCarlo));
        }
    }



}
