using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.RegularExpressions;
using ProbabilisticDatabase.Src.ControllerPackage.Query;
using ProbabilisticDatabase.Src.ControllerPackage.Query.Attribute;
using ProbabilisticDatabase.Src.ControllerPackage.Query.InsertQuery;
using ProbabilisticDatabase.Src.DatabaseEngine;
using ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery;
using ProbabilisticDatabase.Src.ControllerPackage.Query.CreateTableQuery;

namespace PDtests
{
    [TestClass]
    public class RegularExpressionTests
    {
        [TestMethod]
        public void TestNormalSqlCreateTable()
        {
            string sentences = "CREATE TABLE Car (RegId int, Colour varchar(255), Mileage float)";

            SqlCreateTableQuery query = new SqlCreateTableQuery(sentences);

            Assert.IsTrue(query.TableName == "Car");
            Assert.IsTrue(query.AttributeNames.Count == 3);
            Assert.IsTrue(query.AttributeNames[0] == "RegId");
            Assert.IsTrue(query.AttributeNames[1] == "Colour");
            Assert.IsTrue(query.AttributeNames[2] == "Mileage");
            Assert.IsTrue(query.AttributeTypes[0] == "int");
            Assert.IsTrue(query.AttributeTypes[1] == "varchar(255)");
            Assert.IsTrue(query.AttributeTypes[2] == "float");
        }

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

            ProbabilisticSingleAttribute p = (ProbabilisticSingleAttribute)query.Attributes[1];
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

            ProbabilisticSingleAttribute p = (ProbabilisticSingleAttribute)query.Attributes[1];
            Assert.IsTrue(p.Values[0] == "785");
            Assert.IsTrue(p.Probs[0] == 25.0);
            Assert.IsTrue(p.Values[1] == "185");
            Assert.IsTrue(p.Probs[1] == 70.0);
        }

        [TestMethod]
        public void TestInsertWithFullyDependentColumns()
        {
            string sentences = "INSERT INTO socialData(att1,att3,att2,att4) VALUES (351,Smith,PROBABLY [785 Single] 25%/ [185 Married] 75%)";

            SqlInsertQuery query = new SqlInsertQuery(sentences);

            query.processAndPopulateEachField();

            Assert.IsTrue((int)query.TupleP == 100);
            Assert.IsTrue(query.TableName == "socialData");
            Assert.IsTrue(query.Attributes.Count == 3);
            var multiColumn = (ProbabilisticMultiAttribute)query.Attributes[2];
            Assert.IsTrue(multiColumn.MultiAttrbutes.Count == 2);

            Assert.IsTrue((int)multiColumn.PValues[0] == 25);
            Assert.IsTrue((int)multiColumn.PValues[1] == 75);

            var value1 = multiColumn.MultiAttrbutes[0];
            Assert.IsTrue(value1.Count == 2);
            Assert.IsTrue(value1[0] == "785");
            Assert.IsTrue(value1[1] == "Single");

            var value2 = multiColumn.MultiAttrbutes[1];
            Assert.IsTrue(value2.Count == 2);
            Assert.IsTrue(value2[0] == "185");
            Assert.IsTrue(value2[1] == "Married");
        }

        [TestMethod]
        public void TestInsertWithColumnNameSpecified()
        {
            string sentences = "INSERT INTO socialData(att1,att3,att2,att4) VALUES (351,Smith,hoho,haha)";

            SqlInsertQuery query = new SqlInsertQuery(sentences);

            query.processAndPopulateEachField();
            var colNames = query.ColNames;

            Assert.IsNotNull(colNames);
            Assert.IsTrue(colNames.Count == 4);
            Assert.IsTrue(colNames[0] == "att1");
            Assert.IsTrue(colNames[1] == "att3");
            Assert.IsTrue(colNames[2] == "att2");
            Assert.IsTrue(colNames[3] == "att4");
        }

        [TestMethod]
        public void TestParsingProbClause()
        {
            string sentences = "785 25% / 185 50% ";
            var pa = (ProbabilisticSingleAttribute)SqlInsertQuery.processProbabilisticValueClause(sentences);

            Assert.IsTrue(pa.Probs.Count == 2);
            Assert.IsTrue(pa.Values.Count == 2);
            Assert.IsTrue(pa.Values[0] == "785");
            Assert.IsTrue(pa.Probs[0] == 25.0);
            Assert.IsTrue(pa.Values[1] == "185");
            Assert.IsTrue(pa.Probs[1] == 50.0);
        }

        [TestMethod]
        public void TestParsingMultiAttributeClause()
        {
            string sentences = "[785 Single] 25%/ [185 Married] 75%";
            var pa = (ProbabilisticMultiAttribute)SqlInsertQuery.processProbabilisticValueClause(sentences);

            Assert.IsNotNull(pa);
            Assert.IsTrue(pa.MultiAttrbutes.Count == 2);
            Assert.IsTrue(pa.PValues.Count == 2);

            var firstValue = pa.MultiAttrbutes[0];
            Assert.IsTrue(firstValue.Count == 2);
            Assert.IsTrue(firstValue[0] == "785");
            Assert.IsTrue(firstValue[1] == "Single");
            Assert.IsTrue((int)pa.PValues[0] == 25);

            var secondValue = pa.MultiAttrbutes[1];
            Assert.IsTrue(secondValue.Count == 2);
            Assert.IsTrue(secondValue[0] == "185");
            Assert.IsTrue(secondValue[1] == "Married");
            Assert.IsTrue((int)pa.PValues[1] == 75);
        }

        [TestMethod]
        public void TestParsingSelectQueryWithWhereClause()
        {
            string sentences = "Select a,b,c from Data where (a=25 and b=15)";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "a=25 and b=15");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.Default));
        }

        [TestMethod]
        public void TestParsingSelectQueryWithWhereClauseAndStrategy()
        {
            string sentences = "Select a,b,c from Data where (a=25 and b=15) Evaluate using (monte Carlo)";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "a=25 and b=15");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.MonteCarlo));
        }

        [TestMethod]
        public void TestParsingSelectQueryWithOnlyStrategy()
        {
            string sentences = "Select a,b,c from Data Evaluate using (monte Carlo)";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.MonteCarlo));
        }

        [TestMethod]
        public void TestParsingSelectQueryWithJoin()
        {
            string sentences = "Select a,b,c from [Data as t1 Join (SELECT * FROM Data2) as t2 on t1.ha=t2.ho]";

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.Default));

            var subquery = query.SubQuery;
            Assert.IsTrue(query.JoinOnAttributes.Trim() == "t1.ha=t2.ho");
            Assert.IsTrue(query.HasSubquery);
            Assert.IsTrue(subquery.HasSubquery == false);
            Assert.IsTrue(subquery.TableName.Trim() == "Data2");
        }

        [TestMethod]
        public void TestParsingSelectQueryWithThreeJoin()
        {
            var secondJoin = string.Format("Select e,f,g from [Data2 as t1 Join (Select * FROM Data3) as t2 on t1.he=t2.hy]");
            string sentences = string.Format("Select a,b,c from [Data as t1 Join ({0}) as t2 on t1.ha=t2.ho]",secondJoin);

            SqlSelectQuery query = new SqlSelectQuery(sentences);

            Assert.IsTrue(query.Attributes.Trim() == "a,b,c");
            Assert.IsTrue(query.TableName.Trim() == "Data");
            Assert.IsTrue(query.ConditionClause.Trim() == "");
            Assert.IsTrue(query.Strategy.Equals(EvaluationStrategy.Default));
            Assert.IsTrue(query.JoinOnAttributes.Trim() == "t1.ha=t2.ho");
            Assert.IsTrue(query.HasSubquery == true);

            var subquery = query.SubQuery;
            Assert.IsTrue(subquery.HasSubquery == true);
            Assert.IsTrue(subquery.TableName.Trim() == "Data2");
            Assert.IsTrue(subquery.Attributes == "e,f,g");
            Assert.IsTrue(subquery.JoinOnAttributes.Trim() == "t1.he=t2.hy");

            var subsubquery = subquery.SubQuery;
            Assert.IsTrue(subsubquery.HasSubquery == false);
            Assert.IsTrue(subsubquery.TableName.Trim() == "Data3");
            Assert.IsTrue(subsubquery.Attributes == "*");

        }
    }



}
