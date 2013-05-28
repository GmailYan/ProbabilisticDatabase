using ProbabilisticDatabase.Src.ControllerPackage.Query;
using ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    public class SelectQueryHandler
    {
        private SqlSelectQuery query;
        private IStandardDatabase underlineDatabase;

        public SelectQueryHandler(Query.SelectQuery.SqlSelectQuery squery, IStandardDatabase underlineDatabase)
        {
            this.query = squery;
            this.underlineDatabase = underlineDatabase;
        }

        /// <summary>
        /// procedure are in 2 stages, stage 1 apply raw Where Clause to get data interested
        /// stage 2, apply evaluation strategy to get the overall result
        /// 
        /// PS:for simple 1 table select, do the original sql query over all possible worlds of
        /// this table in PD, and return results in the descending order of probability
        /// </summary>
        /// <param name="query"></param>
        public DataTable HandleSelectSqlQuery()
        {
            var answerTableName = "needToGetRidOfThisVariable";
            DataTable result = computeJointResultUsingStrategy(query.Attributes, answerTableName, query.Strategy, query);
            return result;
        }


        /// <summary>
        /// Default is Exact method, while monte carlo is sampling using frequency of event occur
        /// </summary>
        /// <param name="attributes"></param>
        /// <param name="answerTableName"></param>
        /// <param name="evaluationStrategy"></param>
        /// <returns></returns>
        private DataTable computeJointResultUsingStrategy(string attributes, string answerTableName, EvaluationStrategy evaluationStrategy, SqlSelectQuery query)
        {
            DataTable result = new DataTable();
            switch (evaluationStrategy)
            {
                case EvaluationStrategy.Default:
                    preparePossibleStatesTable(query);
                    return naiveStrategy(attributes,query);

                case EvaluationStrategy.Exact:
                    var selectSql = string.Format("SELECT {0},Sum(p) as p FROM {1} GROUP BY {0} ORDER BY p DESC", query.Attributes, answerTableName);
                    result = underlineDatabase.ExecuteSqlWithResult(selectSql);
                    return result;
                case EvaluationStrategy.MonteCarlo:
                    //todo: monte carlo not used
                    var samplingResultTable = query.TableName + "_MonteCarloSampling";
                    var samplingRuns = 100;

                    return ExecuteMonteCarloSampling(samplingResultTable, answerTableName, samplingRuns, query); ;
            }
            return result;
        }

        private void preparePossibleStatesTable(SqlSelectQuery query)
        {
            string aggregateTable = query.TableName + "_PossibleWorldsAggregated";
            underlineDatabase.DropTableIfExist(aggregateTable);

            var aggregateSQL = string.Format("SELECT {0} INTO {1} FROM {2}_PossibleWorlds GROUP BY worldNo",
                "worldNo,Exp(Sum(Log(p/100)))*100 as p", aggregateTable,query.TableName);

            underlineDatabase.ExecuteSql(aggregateSQL);
        }

        private DataTable naiveStrategy(string attributes, SqlSelectQuery query)
        {
            var answerTableName = string.Format("{0}_Answer", query.TableName);
            underlineDatabase.DropTableIfExist(answerTableName);

            if (query.ConditionClause != null && query.ConditionClause.Length > 0)
            {
                string applyWhereClause = string.Format("SELECT worldNo,{0},p FROM {1} WHERE {2}",
                    query.Attributes, query.TableName + "_PossibleWorlds", query.ConditionClause);
                string selectInto = string.Format("SELECT * INTO {0} FROM ({1}) as t1",
                    answerTableName, applyWhereClause);
                underlineDatabase.ExecuteSql(selectInto);
            }
            else
            {
                string applyWhereClause2 = string.Format("SELECT worldNo,{0},p INTO {2} FROM {1}",
                    query.Attributes, query.TableName + "_PossibleWorlds", answerTableName);
                underlineDatabase.ExecuteSql(applyWhereClause2);
            }

            var gettingAnswersSQL = string.Format("SELECT DISTINCT worldNo,{0} FROM {1}_Answer",query.Attributes,query.TableName);
            var aggregateAnswerSQL = string.Format("SELECT {0},Sum(t2.p) as p FROM (({1}) as t1 join {2}_PossibleWorldsAggregated as t2 on t1.worldNo=t2.worldNo) GROUP BY {0} ORDER BY p DESC",
                query.Attributes,gettingAnswersSQL,query.TableName);
            return underlineDatabase.ExecuteSqlWithResult(aggregateAnswerSQL);
        }

        //todo: this is the wrong method
        private DataTable NormalisingTableByAttributes(String targetTable,string attribute)
        {
            var tempTableName = "SocialData_Temp";
            underlineDatabase.DropTableIfExist(tempTableName);
            DataTable worldNumbers = underlineDatabase.ExecuteSqlWithResult("SELECT DISTINCT worldNo FROM "+targetTable);

            Boolean firstRow = true;
            foreach( DataRow eachRow in worldNumbers.Rows)
            {
                var worldNo = eachRow.Field<int>("worldNo");
                string sql;
                if (firstRow)
                {
                    firstRow = false;
                    sql = string.Format("SELECT {1} as worldNo,{2},Sum(p) as p INTO {0} FROM {3} WHERE worldNo = {1} GROUP BY {2}",
                        tempTableName, worldNo, attribute, targetTable);

                    underlineDatabase.ExecuteSql(sql);
                }
                else
                {
                    string subSQL = string.Format("SELECT {1} as worldNo,{2},Sum(p) as p FROM {0} WHERE worldNo = {1} GROUP BY {2}",
                        targetTable, worldNo, attribute );

                    sql = string.Format("INSERT INTO {0} {1}",
                        tempTableName,subSQL);

                    underlineDatabase.ExecuteSql(sql);
                }
            }

            string independentProject = string.Format("SELECT {0},dbo.IndependentProject(p) as p FROM {1} GROUP BY {0}", attribute, tempTableName);
            return underlineDatabase.ExecuteSqlWithResult(independentProject);
            
        }

        /// <summary>
        /// turn list<string> into a single comma seperated string
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        private string TurnListToCommaSeperatedString(List<string> list)
        {
            string result = "";
            bool first = true;
            foreach(var s in list)
            {
                if (first)
                {
                    result += s;
                }
                else
                {
                    result += "," + s;
                }

            }
            return result;
        }

        private DataTable ExecuteMonteCarloSampling(string samplingResultTable, string samplingTargetTable, int samplingRuns, SqlSelectQuery query)
        {
            //:Todo not used
            Random random = new Random();

            var sql = String.Format("select count(*) from {0}", samplingTargetTable);
            var result = underlineDatabase.ExecuteSqlWithResult(sql);

            if (result.Rows.Count != 1)
                throw new Exception("");

            var noOfWorlds = (int)result.Rows[0][0];

            DataTable allSample = new DataTable();
            for (int i = 1; i <= samplingRuns; i++)
            {
                var worldNoSelected = random.Next(1, noOfWorlds);
                var selectStringWorld = string.Format("SELECT * FROM {0} WHERE worldNo = {1}", samplingTargetTable, worldNoSelected);
                var aSample = underlineDatabase.ExecuteSqlWithResult(selectStringWorld);
                if (i == 1)
                    allSample = aSample.Clone();

                allSample = addOneTableToAnother(allSample, aSample, random);
            }

            DataTable frenquencyResult = computeFrequencyResult(allSample, samplingResultTable, query.Attributes, samplingRuns, query);

            return frenquencyResult;
        }


        private void WriteTableIntoDatabase(string samplingResultTable, DataTable allSample)
        {
            underlineDatabase.DropTableIfExist(samplingResultTable);

            var attributes = new List<string>();
            var attributeTypes = new List<string>();

            foreach (DataColumn col in allSample.Columns)
            {
                attributes.Add(col.ColumnName);
                attributeTypes.Add(mapCLRTypeToSqlType(col.DataType.Name));
            }
            underlineDatabase.CreateNewTable(samplingResultTable, attributes.ToArray(), attributeTypes.ToArray());
            underlineDatabase.WriteTableBacktoDatabase(samplingResultTable, allSample);
        }

        /// <summary>
        /// save allSample table back to database in order to be able to use group by keyword.
        /// the temporary table called samplingResultTable is created for this purposes
        /// </summary>
        /// <param name="allSample"></param>
        /// <param name="samplingResultTable"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        private DataTable computeFrequencyResult(DataTable allSample, string samplingResultTable, string selectedFields, int noOfSamplingRuns, SqlSelectQuery query)
        {
            WriteTableIntoDatabase(samplingResultTable, allSample);
            var groupingSql = string.Format("SELECT {0},(COUNT(*)/{2}) as p FROM {1} GROUP BY {0}", selectedFields, samplingResultTable, noOfSamplingRuns);
            return underlineDatabase.ExecuteSqlWithResult(groupingSql);
        }

        private DataTable addOneTableToAnother(DataTable allSample, DataTable aSample, Random random)
        {
            foreach (var row in aSample.AsEnumerable())
            {
                // rowProbability should be between 0 to 100
                var rowProbability = row.Field<Double>("p");
                // even this world is select, there is a chance this row would not realise depends on its tuple probability
                var randomVariable = random.NextDouble();
                if ((randomVariable * 100) <= rowProbability)
                {
                    var dr = allSample.NewRow();
                    foreach (DataColumn column in aSample.Columns)
                    {
                        var columnName = column.ColumnName;
                        switch (columnName)
                        {
                            case "worldNo":
                                dr.SetField<int>(columnName, row.Field<int>(columnName));
                                break;
                            case "p":
                                dr.SetField<double>(columnName, row.Field<double>(columnName));
                                break;
                            default:
                                dr.SetField<string>(columnName, row.Field<string>(columnName));
                                break;
                        }

                    }
                    allSample.Rows.Add(dr);
                }
            }
            return allSample;
        }

        private string mapCLRTypeToSqlType(string p)
        {
            switch (p)
            {
                case "Int32":
                    return "int";
                case "Int64":
                    return "int";
                case "String":
                    return "varchar(255)";
                case "Double":
                    return "float";
                default:
                    return "varchar(255)";
            }
        }

    }
}
