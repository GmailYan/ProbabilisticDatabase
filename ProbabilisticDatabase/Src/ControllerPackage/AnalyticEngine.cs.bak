using System.Data;
using ProbabilisticDatabase.Src.ControllerPackage.Query.InsertQuery;
using ProbabilisticDatabase.Src.DatabaseEngine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProbabilisticDatabase.Src.ControllerPackage;
using ProbabilisticDatabase.Src.ControllerPackage.Query;
using ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery;

namespace ProbabilisticDatabase.Src.ControllerPackage
{
    public class AnalyticEngine : IAnalyticEngine
    {
        private IStandardDatabase underlineDatabase = new StandardDatabase();

        /// <summary>
        /// due to the probabilistic nature, we assume all data are of string type,
        /// thus Create table is not required, and engine will handle table creation upon
        /// receive the first insert SQL query
        /// </summary>
        public string submitSQL(string sql)
        {

            SqlQuery rawQuery = new SqlQuery(sql);
            QueryType qType = rawQuery.processType();

            switch (qType){
                case QueryType.INSERT:
                     var query = new SqlInsertQuery(sql);
                     query.processAndPopulateEachField();
                     HandleInsertSqlQuery(query);
                     break;
                case QueryType.SELECT:
                     var squery = new SqlSelectQuery(sql);
                     squery.processAndPopulateEachField();
                     HandleSelectSqlQuery(squery);
                     break;
                default:
                     break;
            }

            return "end of submitSQL function";
        }

        public string submitSQLWithResult(string sql, out DataTable answerSet)
        {

            SqlQuery rawQuery = new SqlQuery(sql);
            QueryType qType = rawQuery.processType();
            answerSet = null;
            switch (qType)
            {
                case QueryType.INSERT:
                    var query = new SqlInsertQuery(sql);
                    query.processAndPopulateEachField();
                    HandleInsertSqlQuery(query);
                    break;
                case QueryType.SELECT:
                    var squery = new SqlSelectQuery(sql);
                    squery.processAndPopulateEachField();
                    answerSet = HandleSelectSqlQuery(squery);
                    break;
                default:
                    break;
            }

            return "end of submitSQL function";
        }

        /// <summary>
        /// for simple 1 table select, do the original sql query over all possible worlds of
        /// this table in PD, and return results in the descending order of probability
        /// </summary>
        /// <param name="query"></param>
        private DataTable HandleSelectSqlQuery(SqlSelectQuery query)
        {
            //todo: handle join and subquery case
            int noOfWorld = underlineDatabase.GetNumberOfPossibleWorlds(query.TableName);
            var answerTableName = string.Format("{0}_Answer", query.TableName);
            
            for (int i = 1; i <= noOfWorld; i++)
            {
                var sql = string.Format("SELECT {0} FROM {1}",
                    query.Attributes + ",worldNo,p", query.TableName + "_PossibleWorlds");
                sql += " WHERE worldNo="+i;
                if (query.ConditionClause != "")
                {
                    sql += " AND " + query.ConditionClause;
                }
                
                if (i==1)
                {
                    // first run has to create the table, subsequent run is just insert
                    string createTableSql = string.Format("SELECT * INTO {0} FROM ({1}) as t1",answerTableName,sql);
                    underlineDatabase.ExecuteSql(createTableSql);
                }
                else
                {
                    string insertSql = string.Format("INSERT INTO {0} {1}", answerTableName, sql);
                    underlineDatabase.ExecuteSql(insertSql);
                }
             }
            var selectSql = string.Format("SELECT {0},dbo.IndependentProject(p) as p FROM {1} GROUP BY {0} ORDER BY p DESC",query.Attributes,answerTableName);
            DataTable result = underlineDatabase.ExecuteSqlWithResult(selectSql);
            return result;
        }

        /// <summary>
        /// its operation is described by google doc chapter 1 storage, 
        /// if table already exist then ignore the create table operation
        /// just do the insert value
        /// </summary>
        /// <param name="query"></param>
        private void HandleInsertSqlQuery(SqlInsertQuery query)
        {
            // only check prime field table here, is it safe enough ? 
            bool isTableExist = underlineDatabase.CheckIsTableAlreadyExist(query.TableName+"_0");
            List<Object> attributes = query.Attributes;

            int randomVariable = 1;

            if (!isTableExist)
            {
                // table creation operation here

                createAttributeTables(query, attributes);
                createPossibleStatesTable(query, attributes);
                createPossibleWorldsTable(query, attributes);
            }
            else
            {
                randomVariable = underlineDatabase.GetNextFreeVariableId(query.TableName);
                if (randomVariable <= 0)
                {
                    // getNextFreeVariableID method fail in some way
                    return;
                }
            }

            // insert value into tables starting here
            InsertAttributeValue(query, attributes, randomVariable);
            PopulatePossibleStatesTable(query, randomVariable);
            PopulatePossibleWorlds(query, randomVariable);
        }

        /// <summary>
        /// Find out all states that a new random variable i can has
        /// for each i's state, duplicate the existing possibleWorld table
        /// and insert this state as a row in every world
        /// </summary>
        /// <param name="query"></param>
        /// <param name="randomVariable"></param>
        private void PopulatePossibleWorlds(SqlInsertQuery query,int randomVariable)
        {
            var tableName = query.TableName;

            var statesTable =  underlineDatabase.ExecuteSqlWithResult("select * from "+tableName+"_PossibleStates");

            IEnumerable<DataRow> newlyAddedPossibleStates = from newRow in statesTable.AsEnumerable()
                                                            where newRow.Field<int>("var") == randomVariable
                                                            select newRow;

            var newVarStates = newlyAddedPossibleStates.Count();

            var existingWorldsTable = underlineDatabase.ExecuteSqlWithResult("select * from "+tableName+"_PossibleWorlds");
            var result = existingWorldsTable.Copy();

            // replicate existingWorldTable, number of copy depends on number of state in new variable.
            var worldNumbers = (from dataRow in existingWorldsTable.AsEnumerable()
                               select dataRow.Field<int>("worldNo")).ToList();

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
                    var duplicatedWorldNumbers = generateNewRandomVariables(worldNumbers, i, existingWorldsTable,ref result);

                    // current new state i is assign to each world
                    foreach (var worldNumber in duplicatedWorldNumbers)
                    {
                        insertVariableState(worldNumber, eachNewState, ref result);
                    }

                }

            }

            underlineDatabase.WriteTableBacktoDatabase(tableName + "_PossibleWorlds", result);
        }

        /// <summary>
        /// if it is 1st duplication, just return the input worldNumbers
        /// otherwise, get the max number from worldNumbers, result = i*max + worldNumbers
        /// </summary>
        /// <param name="worldNumbers"></param>
        /// <param name="ithDuplication"></param>
        /// <param name="existingWorldsTable"></param>
        /// <param name="resultTable"></param>
        /// <returns></returns>
        private List<int> generateNewRandomVariables(List<int> worldNumbers, int ithDuplication,DataTable existingWorldsTable,ref DataTable resultTable)
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
                int newWorldNo = n + max*(ithDuplication - 1);
                result.Add(newWorldNo);

                var toBeReplicated = from i in existingWorldsTable.AsEnumerable()
                where i.Field<int>("worldNo")==n
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
            insertVariableState(newWorldNo,oneWorld,ref resultTable,2);
        }

        // insert a row of possibleStates table into possibleWorlds table
        private void insertVariableState(int worldNo, DataRow dataRow,ref DataTable result,int offset=3)
        {
            var newRow = result.NewRow();
            try
            {
                newRow.SetField("worldNo", worldNo);
                var numberOfColumn = dataRow.ItemArray.Count();
                for (int i = 0; i < numberOfColumn - offset; i++)
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


        /// <summary>
        /// construct and execute sql to populate possible states table using those attribute table. 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="randomVariable"></param>
        private void PopulatePossibleStatesTable(SqlInsertQuery query, int randomVariable)
        {
            var attributes = query.Attributes;
            var numberOfAttributes = attributes.Count;
            var tableName = query.TableName;

            /* sql format is: 
             * 
INSERT INTO socialData_PossibleStates (var,v,att0,att1,att2,p)
SELECT t0.var,row_number() over (order by t0.v,t1.v,t2.v) as v,t0.att0,t1.att1,t2.att2,(t0.p/100)*(t1.p/100)*(t2.p/100)*100 as p
FROM socialData_0 as t0 cross join socialData_1 as t1 cross join socialData_2 as t2 
WHERE t0.var=2 and t0.var = t1.var and t0.var = t2.var 
             
            */

            string attributeClause = "var,v";
            string fromClause = "";
            string whereClause = string.Format(" t0.var={0} and ",randomVariable);
            string combiningVField = "";
            string combiningAttField = "";
            string combiningPField = "";
            for (int i = 0; i < numberOfAttributes; i++)
            {
                attributeClause += " ,att"+i;
                combiningPField += string.Format(" (t{0}.p/100)* ", i); 
                if (i==0)
                {
                    fromClause += string.Format(" {0}_0 as t0 ",tableName);
                    combiningVField += " t0.v ";
                    combiningAttField += " t0.att0 ";
                }
                else
                {
                    fromClause += string.Format(" cross join {0}_{1} as t{1} ", tableName,i);
                    combiningVField += " ,t"+i+".v ";
                    combiningAttField += string.Format(" ,t{0}.att{0} ",i);
                    if ( i==1)
                    {
                        whereClause += string.Format(" t0.var = t{0}.var ", i);
                    }
                    else
                    {
                        whereClause += string.Format(" and t0.var = t{0}.var ", i);
                    }
                   
                }
            }
            attributeClause += ",p";

            string selectClause = string.Format("t0.var,row_number() over (order by {0}) as v,{1},{2}100 as p",
                combiningVField,combiningAttField,combiningPField);
            string sql = string.Format("insert into {0}_PossibleStates ({1}) select {2} from {3} where {4}",
                tableName, attributeClause, selectClause, fromClause, whereClause);

            var result = underlineDatabase.ExecuteSql(sql);

            //todo: can call front end to display the result

        }

        private void InsertAttributeValue(SqlInsertQuery query, List<Object> attributes, int randomVariable)
        {
            for (int i = 0; i < attributes.Count; i++)
            {
                string attributeTableName = query.TableName + "_" + i;

                var value = attributes[i] as AttributeValue;
                if (value != null)
                {
                    AttributeValue attribute = value;
                    double prob = 0;
                    prob = i == 0 ? query.TupleP : 100;

                    underlineDatabase.InsertValueIntoAttributeTable(attributeTableName, randomVariable, 1, attribute.AttributeValue1, prob);
                }
                else
                {
                    var probabilisticAttribute = attributes[i] as ProbabilisticAttribute;
                    if (probabilisticAttribute != null)
                    {
                        var attribute = probabilisticAttribute;
                        List<String> v = attribute.Values;
                        List<double> p = attribute.Probs;

                        for (int j = 0; j < v.Count; j++)
                        {
                            // attribute value starting from 1 upto number of possible values, 
                            // because 0 is system reserve for null state.
                            underlineDatabase.InsertValueIntoAttributeTable(attributeTableName, randomVariable, j + 1, v[j], p[j]);
                        }

                    }
                }
            }
        }

        private void createPossibleStatesTable(SqlInsertQuery query, List<object> attributes)
        {
            String[] attributeNames = { "var", "v" };
            String[] attributeTypes = { "INT", "INT" };
            string attributeTableName = query.TableName + "_PossibleStates";

            List<String> attributeNamesList = attributeNames.ToList();
            List<String> attributeTypesList = attributeTypes.ToList();

            for (int i = 0; i < attributes.Count; i++)
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

        private void createPossibleWorldsTable(SqlInsertQuery query, List<object> attributes)
        {
            String[] attributeNames = { "worldNo" };
            String[] attributeTypes = { "INT" };
            string attributeTableName = query.TableName + "_PossibleWorlds";

            List<String> attributeNamesList = attributeNames.ToList();
            List<String> attributeTypesList = attributeTypes.ToList();

            for (int i = 0; i < attributes.Count; i++)
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

        private void createAttributeTables(SqlInsertQuery query, List<Object> attributes)
        {
            for (int i = 0; i < attributes.Count; i++)
            {
                string attributeTableName = query.TableName + "_" + i;
                string ai = "att" + i;
                String[] attributeNames = { "var", "v", ai , "p" };
                String[] attributeTypes = { "INT", "INT", "NVARCHAR(MAX)", "float" };
                underlineDatabase.CreateNewTable(attributeTableName, attributeNames, attributeTypes);
            }
        }


        public System.Data.DataTable viewTable(string tableName)
        {
            string sql = "Select * From " + tableName;
            return underlineDatabase.ExecuteSqlWithResult(sql);
        }
    }
}
