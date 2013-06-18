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
        private const string answerTableName = "answer";

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
                    List<string> tables = _query.GetRevelentTables();
                    PrepareReleventTables(tables);
                    underlineDatabase.DropTableIfExist(answerTableName);
                    OrderedSetIterator iterator = new OrderedSetIterator(tables, underlineDatabase);
                    while (iterator.HasNext())
                    {
                        var worldNoTuple = iterator.NextSetOfWorldNo();
                        var worldNo = worldNoTuple.Item1;
                        var probability = worldNoTuple.Item2;
                        for (int i = 0; i < worldNo.Count; i++ )
                        {
                            ConvertWorldToTable(tables[i], worldNo[i]);
                        }
                        //TODO: need to get rid of strategy in ProSQL for execution on DB, or utilise default strategy
                        var a = underlineDatabase.ExecuteSqlWithResult(_query.Sql);
                        WriteResultToAnswerTable(iterator.GetIndex(),a,probability);
                    }
                    result = NormalisingTableByAttributes(answerTableName);
                    return result;
                case EvaluationStrategy.Naive:
                    result = HandleSelectSqlQuery(false);
                    return result;

                case EvaluationStrategy.Extensional:
                    return result;
            }
            return result;

         //   return HandleSelectSqlQuery(false);
        }

        public void PrepareReleventTables(List<string> tables)
        {
            foreach (var table in tables)
            {
                underlineDatabase.DropTableIfExist(table + "_PossibleStates");
                underlineDatabase.DropTableIfExist(table + "_PossibleWorlds");
                int attributeSize = PreparePossibleStatesTable(table);
                CreatePossibleWorldsTable(table, attributeSize);
                PreparePossibleWorldsTable(table);
                PreparePossibleWorldsAggregatedTable(table);
            }
        }

        private DataTable NormalisingTableByAttributes(string targetTable)
        {
            var sql = string.Format("select top 1 * from {0}", targetTable);
            var r = underlineDatabase.ExecuteSqlWithResult(sql);
            var cols = r.Columns;
            List<string> attributes = new List<string>();
            // select all attributes except 1st and last, they are worldNo and p
            for (int i = 1; i < (cols.Count - 1); i++)
            {
                attributes.Add(cols[i].ColumnName);
            }
            return NormalisingTableByAttributes(targetTable, string.Join(",", attributes));
        }

        private void WriteResultToAnswerTable(int worldNo, DataTable content, double probability)
        {
            DataTable answerTable = new DataTable();
            answerTable.Columns.Add(new DataColumn("worldNo", typeof (int)));
            var attCols = content.Columns;
            var colNames = new List<string>();
            foreach (DataColumn col in attCols)
            {
                answerTable.Columns.Add(col.ColumnName,typeof(string));
                colNames.Add(col.ColumnName);
            }
            answerTable.Columns.Add(new DataColumn("p", typeof (double)));

            var distinctRows = content.DefaultView.ToTable(true, colNames.ToArray());
            foreach (DataRow row in distinctRows.Rows)
            {
                var newRow = answerTable.NewRow();
                newRow.SetField("worldNo", worldNo);
                newRow.SetField("p", probability);

                foreach (DataColumn column in content.Columns)
                {
                    var columnName = column.ColumnName;
                    newRow.SetField(columnName, row.Field<string>(columnName));
                }
                answerTable.Rows.Add(newRow);
            }
            if (underlineDatabase.CheckIsTableAlreadyExist(answerTableName))
            {
                underlineDatabase.WriteTableBacktoDatabase(answerTableName, answerTable);
            }
            else
            {
                underlineDatabase.CreateNewTableWithDataTable(answerTableName, answerTable);
            }
        }

        private void ConvertWorldToTable(string tableName, int worldNo)
        {
            underlineDatabase.DropTableIfExist(tableName);
            // could store history enabling skip those case that table already in worldNo
            var sql = string.Format("Select * Into {0} from {0}_PossibleWorlds Where worldNo={1}", tableName, worldNo);
            underlineDatabase.ExecuteSql(sql);
        }

        /// <summary>
        /// procedure are in 2 stages, stage 1 apply raw Where Clause to get data interested
        /// stage 2, apply evaluation strategy to get the overall result
        /// 
        /// PS:for simple 1 table select, do the original sql query over all possible worlds of
        /// this table in PD, and return results in the descending order of probability
        /// </summary>
        /// <param name="isIntermediateResult"> </param>
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

                var attributeSelected = AttbutesWithoutRenaming(_query.Attributes);
                JoinPossibleWorlds(table1 + "_PossibleWorlds", "subquery_PossibleWorlds", "answer_PossibleWorlds",
                    _query.JoinOnAttributes, attributeSelected);
                JoinPossibleWorldsAggregatedTable(table1+"_PossibleWorldsAggregated", "subquery_PossibleWorldsAggregated", "answer_PossibleWorldsAggregated");

                DataTable result = NaiveStrategy(_query.Attributes, _query.ConditionClause, "answer",isIntermediateResult);
                return result;
            }
            else
            {
                // no more subquery/join, now just consider a single table
                var answerTableName = "needToGetRidOfThisVariable";
                DataTable result = ComputeJointResultUsingNaiveStrategy(_query.Attributes, _query,isIntermediateResult);
                return result;
            }
        }

        /// <summary>
        /// attribute could be of the form: att1 as t2att1, this function get rid of renaming
        /// </summary>
        /// <param name="attributesWithRenaming"></param>
        /// <returns></returns>
        private string AttbutesWithoutRenaming(string attributesWithRenaming)
        {
            var resultList = new List<string>();
            string sPattern = @"\s*(\A|,)\s*(?<att>.+?)\s*(?=(AS|,|\z))";
            var matches = Regex.Matches(attributesWithRenaming, sPattern, RegexOptions.IgnoreCase);
            for (int i = 0; i < matches.Count; i++)
            {
                var att = matches[i].Groups["att"].Value;
                resultList.Add(att);
            }
            return string.Join(",", resultList);
        }

        private void JoinPossibleWorlds(string t1, string t2, string resultTable, string joinCondition, string attributeSelected)
        {
            DataTable selectResult = underlineDatabase.ExecuteSqlWithResult("SELECT count(*) FROM (SELECT DISTINCT worldNo FROM "+t2+") as tt");
            var t2WorldSize = selectResult.Rows[0][0].ToString();

            var joinWorldNo = string.Format("(t1.worldNo-1)*{0} + t2.worldNo as worldNo",t2WorldSize);
            var joinProbability = string.Format("(t1.p/100)*(t2.p/100)*100 as p");
            var joinSql = string.Format("SELECT {4},{0},{5} FROM {1} as t1 inner join {2} as t2 on {3}",
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
        /// Default is Extensional method, while monte carlo is sampling using frequency of event occur
        /// </summary>
        /// <param name="attributes"> must specify what they are, wildcard like * not supported !</param>
        /// <returns></returns>
        private DataTable ComputeJointResultUsingNaiveStrategy(string attributes, SqlSelectQuery query, bool intermediate)
        {
            int attributeSize = PreparePossibleStatesTable(query.TableName);
            CreatePossibleWorldsTable(query.TableName, attributeSize);
            PreparePossibleWorldsTable(query.TableName);
            PreparePossibleWorldsAggregatedTable(query.TableName);
            return NaiveStrategy(attributes,query.ConditionClause,query.TableName,intermediate);

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
            var statesTable = underlineDatabase.ExecuteSqlWithResult("select * from " + tableName + "_PossibleStates");

            foreach (DataRow row in getVariblesResult.Rows)
            {
                var variable = row.Field<int>("var");
                GeneratePossibleWorlds(tableName, variable, statesTable);
            }

        }

        private void GeneratePossibleWorlds(string tableName, int randomVariable, DataTable statesTable)
        {
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

            if(intermediate)
            {
                var result = underlineDatabase.ExecuteSqlWithResult("SELECT * FROM " + answerTableName);
                underlineDatabase.CreateNewTableWithDataTable("subquery_PossibleWorlds", result);
                var possibleWorldsAggregatedTable = tableName + "_PossibleWorldsAggregated";
                var result2 = underlineDatabase.ExecuteSqlWithResult("SELECT * FROM " + possibleWorldsAggregatedTable + 
                    " WHERE worldNo IN (Select WorldNo from " + answerTableName+")");
                //PreparePossibleWorldsAggregatedTable("subquery");
                underlineDatabase.CreateNewTableWithDataTable("subquery_PossibleWorldsAggregated", result2);
                return result;
            }

            var gettingAnswersSQL = string.Format("SELECT DISTINCT worldNo,{0} FROM {1}_Answer", attributes, tableName);
            var aggregateAnswerSQL = string.Format("SELECT {0},Sum(t2.p) as p FROM (({1}) as t1 join {2}_PossibleWorldsAggregated as t2 on t1.worldNo=t2.worldNo) GROUP BY {0} ORDER BY p DESC",
                attributes, gettingAnswersSQL, tableName);
            return underlineDatabase.ExecuteSqlWithResult(aggregateAnswerSQL);
        }

        /// <summary>
        /// normal relational project + independent project
        /// </summary>
        /// <param name="targetTable"></param>
        /// <param name="attribute"></param>
        /// <returns></returns>
        private DataTable NormalisingTableByAttributes(String targetTable,string attribute)
        {
            string independentProject = string.Format("SELECT {0},Sum(p) as p FROM {1} GROUP BY {0} ORDER BY p DESC", attribute, targetTable);
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
