using System.Text.RegularExpressions;
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
        private SqlSelectQuery _query;
        private IStandardDatabase underlineDatabase;

        public SelectQueryHandler(SqlSelectQuery squery, IStandardDatabase underlineDatabase)
        {
            _query = squery;
            this.underlineDatabase = underlineDatabase;
        }

        public DataTable HandleSelectSqlQuery()
        {
            DataTable result = new DataTable();
            var evaluationStrategy = _query.Strategy;
            switch (evaluationStrategy)
            {
                case EvaluationStrategy.Lazy:
                case EvaluationStrategy.Default:
                    List<string> tables = GetRevelentTables(_query.Sql);
                    OrderedSetIterator iterator = new OrderedSetIterator(tables, underlineDatabase);
                    while (iterator.hasNext())
                    {
                        List<int> worldNo = iterator.nextSetOfWorldNo();
                        for (int i = 0; i < worldNo.Count; i++ )
                        {
                            ConvertWorldToTable(tables[i], worldNo[i]);
                        }
                        //TODO: need to get rid of strategy in ProSQL for execution on DB
                        var a = underlineDatabase.ExecuteSqlWithResult(_query.Sql);
                        WriteResultToAnswerTable(iterator.GetIndex(),a,iterator.GetJointProbability());
                    }
                    result = NormalisingTableByAttributes("asd","asd");
                    return result;
          }
            return result;

         //   return HandleSelectSqlQuery(false);
        }

        private void WriteResultToAnswerTable(object p1, DataTable a, object p2)
        {
            throw new NotImplementedException();
        }

        private void ConvertWorldToTable(string p1, int p2)
        {
            throw new NotImplementedException();
        }

        private List<string> GetRevelentTables(string p)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// procedure are in 2 stages, stage 1 apply raw Where Clause to get data interested
        /// stage 2, apply evaluation strategy to get the overall result
        /// 
        /// PS:for simple 1 table select, do the original sql query over all possible worlds of
        /// this table in PD, and return results in the descending order of probability
        /// </summary>
        /// <param name="query"></param>
        public DataTable HandleSelectSqlQuery(bool isIntermediateResult)
        {
            if (_query.HasSubquery)
            {
                var table1 = _query.TableName;
                int attributeSize = PreparePossibleStatesTable(table1);
                CreatePossibleWorldsTable(table1, attributeSize);
                PreparePossibleWorldsTable(table1);
                PreparePossibleWorldsAggregatedTable(table1);

                var subQueryHandler = new SelectQueryHandler(_query.SubQuery, underlineDatabase);
                var table2 = subQueryHandler.HandleSelectSqlQuery(true);
                underlineDatabase.CreateNewTableWithDataTable("subquery_PossibleWorlds", table2);
                PreparePossibleWorldsAggregatedTable("subquery");
                
                JoinPossibleWorlds(table1 + "_PossibleWorlds", "subquery_PossibleWorlds", "answer_PossibleWorlds",
                    _query.JoinOnAttributes,_query.Attributes);
                JoinPossibleWorldsAggregatedTable(table1+"_PossibleWorldsAggregated", "subquery_PossibleWorldsAggregated", "answer_PossibleWorldsAggregated");

                DataTable result = NaiveStrategy(_query.Attributes, _query.ConditionClause, "answer",isIntermediateResult);
                return result;
            }
            else
            {
                // no more subquery/join, now just consider a single table
                var answerTableName = "needToGetRidOfThisVariable";
                DataTable result = ComputeJointResultUsingStrategy(_query.Attributes, answerTableName, _query.Strategy, _query,isIntermediateResult);
                return result;
            }
        }

        private void JoinPossibleWorlds(string t1, string t2, string resultTable, string joinCondition, string attributeSelected)
        {
            DataTable selectResult = underlineDatabase.ExecuteSqlWithResult("SELECT count(*) FROM (SELECT DISTINCT worldNo FROM "+t2+") as tt");
            var t2WorldSize = selectResult.Rows[0][0].ToString();

            var joinWorldNo = string.Format("(t1.worldNo-1)*{0} + t2.worldNo as worldNo",t2WorldSize);
            var joinProbability = string.Format("(t1.p/100)*(t2.p/100)*100 as p");
            var joinSql = string.Format("SELECT {4},{0},{5} FROM {1} as t1 full join {2} as t2 on {3}",
                attributeSelected, t1, t2, joinCondition, joinWorldNo, joinProbability);

            var joinedTable = underlineDatabase.ExecuteSqlWithResult(joinSql);
            underlineDatabase.CreateNewTableWithDataTable(resultTable,joinedTable);
        }

        private void JoinPossibleWorldsAggregatedTable(string t1, string t2, string resultTable)
        {
            var joinSql = string.Format("select ROW_NUMBER() over (order by t1.worldNo,t2.worldNo) as worldNo ,(t1.p/100)*(t2.p/100)*100 as p " +
                                        "FROM {0} as t1 cross join {1} as t2",t1,t2);
            DataTable joinResult = underlineDatabase.ExecuteSqlWithResult(joinSql);
            underlineDatabase.CreateNewTableWithDataTable(resultTable,joinResult);
        }


        /// <summary>
        /// Default is Exact method, while monte carlo is sampling using frequency of event occur
        /// </summary>
        /// <param name="attributes"> must specify what they are, wildcard like * not supported !</param>
        /// <param name="answerTableName"></param>
        /// <param name="evaluationStrategy"></param>
        /// <returns></returns>
        private DataTable ComputeJointResultUsingStrategy(string attributes, string answerTableName, EvaluationStrategy evaluationStrategy, SqlSelectQuery query, bool intermediate)
        {
            DataTable result = new DataTable();
            switch (evaluationStrategy)
            {
                case EvaluationStrategy.Default:
                    int attributeSize = PreparePossibleStatesTable(query.TableName);
                    CreatePossibleWorldsTable(query.TableName, attributeSize);
                    PreparePossibleWorldsTable(query.TableName);
                    PreparePossibleWorldsAggregatedTable(query.TableName);
                    return NaiveStrategy(attributes,query.ConditionClause,query.TableName,intermediate);

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

        /// <summary>
        /// To generate possible worlds, for each tuple's possible state, cross join with other tuples' possible states 
        /// </summary>
        /// <param name="tableName"></param>
        private void PreparePossibleWorldsTable(string tableName)
        {
            if (!underlineDatabase.CheckIsTableAlreadyExist(tableName + "_PossibleStates"))
            {
                return;
            }

            var getVariables = string.Format("SELECT var FROM {0}_0", tableName);
            var getVariblesResult = underlineDatabase.ExecuteSqlWithResult(getVariables);
            foreach (DataRow row in getVariblesResult.Rows)
            {
                var variable = row.Field<int>("var");
                GeneratePossibleWorlds(tableName, variable);
            }

        }

        private void GeneratePossibleWorlds(string tableName, int randomVariable)
        {
            var statesTable = underlineDatabase.ExecuteSqlWithResult("select * from " + tableName + "_PossibleStates");

            IEnumerable<DataRow> newlyAddedPossibleStates = from newRow in statesTable.AsEnumerable()
                                                            where newRow.Field<int>("var") == randomVariable
                                                            select newRow;

            var newVarStates = newlyAddedPossibleStates.Count();

            var possibleWorldsTable = tableName + "_PossibleWorlds";
            bool tableExists = underlineDatabase.CheckIsTableAlreadyExist(possibleWorldsTable);
            if (!tableExists)
            {
                throw new Exception(possibleWorldsTable+"table not created yet");
            }
            var existingWorldsTable = underlineDatabase.ExecuteSqlWithResult("select * from " + possibleWorldsTable);
            var result = existingWorldsTable.Copy();

            // replicate existingWorldTable, number of copy depends on number of state in new variable.
            var worldNumbers = (from dataRow in existingWorldsTable.AsEnumerable()
                                select dataRow.Field<int>("worldNo")).Distinct().ToList();

            // if old worlds is empty, for each new state create a world
            if (!worldNumbers.Any())
            {
                int index = 0;
                foreach (var eachPossibleState in newlyAddedPossibleStates)
                {
                    index++;
                    insertVariableState(index, eachPossibleState, ref result);
                }
            }
            else
            {
                for (int i = 1; i <= newVarStates; i++)
                {
                    var eachNewState = newlyAddedPossibleStates.ElementAt(i - 1);
                    // for each new variable state, duplicate existing worlds
                    var duplicatedWorldNumbers = generateNewRandomVariables(worldNumbers, i, existingWorldsTable, ref result);

                    // current new state i is assign to each world
                    foreach (var worldNumber in duplicatedWorldNumbers)
                    {
                        insertVariableState(worldNumber, eachNewState, ref result);
                    }

                }

            }

            underlineDatabase.WriteTableBacktoDatabase(tableName + "_PossibleWorlds", result);
        }

        private void CreatePossibleWorldsTable(string tableName, int attributeSize)
        {
            String[] attributeNames = { "worldNo" };
            String[] attributeTypes = { "INT" };
            string attributeTableName = tableName + "_PossibleWorlds";

            List<String> attributeNamesList = attributeNames.ToList();
            List<String> attributeTypesList = attributeTypes.ToList();

            for (int i = 1; i <= attributeSize; i++)
            {
                string ai = "att" + i;
                attributeNamesList.Add(ai);
                attributeTypesList.Add("NVARCHAR(MAX)");
            }

            // p is the last attribute
            attributeNamesList.Add("p");
            attributeTypesList.Add("float");

            underlineDatabase.CreateNewTable(attributeTableName, attributeNamesList.ToArray(), attributeTypesList.ToArray());
        }

        private void insertVariableState(int worldNo, DataRow dataRow, ref DataTable result, int offset = 2)
        {
            var newRow = result.NewRow();
            try
            {
                newRow.SetField("worldNo", worldNo);
                var numberOfColumn = dataRow.ItemArray.Count();
                for (int i = 1; i <= numberOfColumn - offset; i++)
                {
                    newRow.SetField("att" + i, dataRow.Field<string>("att" + i));
                }

                newRow.SetField("p", dataRow.Field<Double>("p"));
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            result.Rows.Add(newRow);
        }

        private List<int> generateNewRandomVariables(List<int> worldNumbers, int ithDuplication, DataTable existingWorldsTable, ref DataTable resultTable)
        {
            if (ithDuplication == 1)
            {
                return worldNumbers;
            }

            var max = worldNumbers.Max();
            //what if max is 0;

            var result = new List<int>();
            foreach (var n in worldNumbers)
            {
                // for example, for input ([1,2,3],3) the output is [7,8,9]
                int newWorldNo = n + max * (ithDuplication - 1);
                result.Add(newWorldNo);

                var toBeReplicated = from i in existingWorldsTable.AsEnumerable()
                                     where i.Field<int>("worldNo") == n
                                     select i;

                foreach (var oneWorld in toBeReplicated)
                {
                    InsertVariableWorld(newWorldNo, oneWorld, ref resultTable);
                }

            }

            return result;
        }

        private void InsertVariableWorld(int newWorldNo, DataRow oneWorld, ref DataTable resultTable)
        {
            insertVariableState(newWorldNo, oneWorld, ref resultTable, 2);
        }

        /// <summary>
        /// construct and execute sql to populate possible states table using those attribute table. 
        /// </summary>
        /// <param name="tableName"></param>
        private int PreparePossibleStatesTable(string tableName)
        {
            if (!underlineDatabase.CheckIsTableAlreadyExist(tableName+"_0"))
            {
                throw new Exception(tableName+"_0 table doesn't exist" );
            }
            List<int> attributeSize = new List<int>();
            var getVariables = string.Format("SELECT var,att0,p FROM {0}_0",tableName);
            var getVariblesResult = underlineDatabase.ExecuteSqlWithResult(getVariables);
            foreach (DataRow row in getVariblesResult.Rows)
            {
                var variable = row.Field<int>("var");
                var tableNameString = row.Field<string>("att0");
                List<String> tableNames = new List<string>(tableNameString.Split(','));
                var p = row.Field<double>("p");
                var colSize = GeneratePossibleStates(tableName, variable, tableNames, p);
                attributeSize.Add(colSize);
            }

            if((int)attributeSize.Average() == attributeSize[0])
            {
                return attributeSize[0];
            }
            else
            {
                throw new Exception("inconsistency in attribute sizes of different tuples");
            }
        }

        /// <summary>
        /// return number of attributes implied by tableNames
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="variable"></param>
        /// <param name="tableNames"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        private int GeneratePossibleStates(string tableName, int variable, List<string> tableNames, double p)
        {
            string selectClause = "t0.var,";
            string probabilityClause = "100*(t0.p/100)";
            string joinClause = tableName +"_0 t0";
            List<string> emptyString = new List<string>();
            var colIndex = 1;
            for (int i = 0; i < tableNames.Count; i++)
            {
                var tName = tableNames[i];
                var aProbability = string.Format("*({0}.p/100)", tName);
                probabilityClause += aProbability;
                var aJoin = string.Format(" JOIN {0} ON t0.var = {0}.var",tName);
                joinClause += aJoin;
                int numberOfDigits = NumberOfDigits(tName);
                for (int j = 0; j < numberOfDigits; j++)
                {
                    selectClause += "att" + (colIndex)+",";
                    emptyString.Add("null");
                    colIndex++;
                }
            }

            var statesTable = tableName + "_PossibleStates";
            var tableAlreadyExist = underlineDatabase.CheckIsTableAlreadyExist(statesTable);
            if(tableAlreadyExist)
            {
                var sql = string.Format("INSERT INTO {2} SELECT {0}{1} as p FROM {3} WHERE t0.var={4}",
                    selectClause, probabilityClause, statesTable, joinClause, variable);
                underlineDatabase.ExecuteSql(sql);
            }
            else
            {
                var sql = string.Format("SELECT {0}{1} as p INTO {2} FROM {3} WHERE t0.var={4}",
                    selectClause, probabilityClause, statesTable, joinClause, variable);
                underlineDatabase.ExecuteSql(sql);
            }

            if((int)Math.Round(p) < 100)
            {
                var sql = string.Format("INSERT INTO {0} VALUES ({2},{1},{3})",
                    statesTable, string.Join(",",emptyString),variable,p);
                underlineDatabase.ExecuteSql(sql);
            }
            return colIndex-1;
        }

        private int NumberOfDigits(string tableName)
        {
            const string sPattern = @"(?<digit>\d)";
            var matches = Regex.Matches(tableName, sPattern, RegexOptions.IgnoreCase);
            return matches.Count;
        }

        private void PreparePossibleWorldsAggregatedTable(string tableName)
        {
            string aggregateTable = tableName + "_PossibleWorldsAggregated";
            underlineDatabase.DropTableIfExist(aggregateTable);

            var aggregateSQL = string.Format("SELECT {0} INTO {1} FROM {2}_PossibleWorlds GROUP BY worldNo",
                "worldNo,Exp(Sum(Log(p/100)))*100 as p", aggregateTable, tableName);

            underlineDatabase.ExecuteSql(aggregateSQL);
        }

        /// <summary>
        /// what naive strategy does, is form possibleWorlds, select only worlds that are
        /// relavent to query, and join the probability of those worlds
        /// </summary>
        /// <param name="attributes"></param>
        /// <param name="whereCondition"></param>
        /// <param name="tableName"> expect tableName_PossibleWorlds and tableName_PossibleWorldsAggregated already prepared</param>
        /// <returns></returns>
        private DataTable NaiveStrategy(string attributes , string whereCondition, string tableName, bool intermediate)
        {
            var answerTableName = string.Format("{0}_Answer", tableName);
            underlineDatabase.DropTableIfExist(answerTableName);

            if (!string.IsNullOrEmpty(whereCondition))
            {
                string applyWhereClause = string.Format("SELECT worldNo,{0},p FROM {1} WHERE {2}",
                    attributes, tableName + "_PossibleWorlds", whereCondition);
                string selectInto = string.Format("SELECT * INTO {0} FROM ({1}) as t1",
                    answerTableName, applyWhereClause);
                underlineDatabase.ExecuteSql(selectInto);
            }
            else
            {
                string applyWhereClause2 = string.Format("SELECT worldNo,{0},p INTO {2} FROM {1}",
                    attributes, tableName + "_PossibleWorlds", answerTableName);
                underlineDatabase.ExecuteSql(applyWhereClause2);
            }

            if(intermediate){
                return underlineDatabase.ExecuteSqlWithResult("SELECT * FROM " + answerTableName);
            }

            var gettingAnswersSQL = string.Format("SELECT DISTINCT worldNo,{0} FROM {1}_Answer", attributes, tableName);
            var aggregateAnswerSQL = string.Format("SELECT {0},Sum(t2.p) as p FROM (({1}) as t1 join {2}_PossibleWorldsAggregated as t2 on t1.worldNo=t2.worldNo) GROUP BY {0} ORDER BY p DESC",
                attributes, gettingAnswersSQL, tableName);
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
