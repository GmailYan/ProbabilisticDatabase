using ProbabilisticDatabase.Src.DatabaseEngine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProbabilisticDatabase.Src.ControllerPackage;
using ProbabilisticDatabase.Src.ControllerPackage.Query;

namespace ProbabilisticDatabase
{
    class AnalyticEngine : IAnalyticEngine
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
                     SqlInsertQuery query = new SqlInsertQuery(sql);
                     query.processAndPopulateEachField();
                     HandleInsertSQLQuery(query);
                     break;
                default:
                     break;
            }

            return "end of submitSQL function";
        }

        /// <summary>
        /// its operation is described by google doc chapter 1 storage, 
        /// if table already exist then ignore the create table operation
        /// just do the insert value
        /// </summary>
        /// <param name="query"></param>
        private void HandleInsertSQLQuery(SqlInsertQuery query)
        {
            // only check prime field table here, is it safe enough ? 
            bool isTableExist = underlineDatabase.checkIsTableAlreadyExist(query.TableName+"_0");
            List<Object> attributes = query.Attributes;

            int randomVariable = 1;
            if (!isTableExist)
            {
                // table creation operation here

                createAttributeTables(query, attributes);
                //createPossibleStatesTable(query, attributes);
                //createPossibleWorldsTable(query, attributes);
            }
            else
            {
                randomVariable = underlineDatabase.getNextFreeVariableID(query.TableName);
                if (randomVariable <= 0)
                {
                    // getNextFreeVariableID method fail in some way
                    return;
                }
            }

            // insert value into tables starting here
            InsertAttributeValue(query, attributes, randomVariable);
            
            // TODO: possible states and worlds are left to implement upon on maria's approval 

        }

        private void InsertAttributeValue(SqlInsertQuery query, List<Object> attributes, int randomVariable)
        {
            for (int i = 0; i < attributes.Count; i++)
            {
                string attributeTableName = query.TableName + "_" + i;

                if (attributes[i] is AttributeValue)
                {
                    AttributeValue attribute = (AttributeValue)attributes[i];
                    double prob = 0;
                    if (i == 0)
                    {
                        prob = query.TupleP;
                    }
                    else
                    {
                        prob = 100;
                    }

                    underlineDatabase.insertValueIntoAttributeTable(attributeTableName, randomVariable, 1, attribute.AttributeValue1, prob);
                }
                else if (attributes[i] is ProbabilisticAttribute)
                {
                    ProbabilisticAttribute attribute = (ProbabilisticAttribute)attributes[i];
                    List<String> v = attribute.Values;
                    List<double> p = attribute.Probs;

                    for (int j = 0; j < v.Count; j++)
                    {
                        // attribute value starting from 1 upto number of possible values, 
                        // because 0 is system reserve for null state.
                        underlineDatabase.insertValueIntoAttributeTable(attributeTableName, randomVariable, j + 1, v[j], p[j]);
                    }

                }

            }
        }

        private void createPossibleWorldsTable(SqlInsertQuery query, List<object> attributes)
        {
            String[] attributeNames = { "var", "v" };
            String[] attributeTypes = { "INT", "INT" };
            string attributeTableName = query.TableName + "_PossibleWorld";

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

            underlineDatabase.createNewTable(attributeTableName, attributeNames, attributeTypes);
        }

        private void createPossibleStatesTable(SqlInsertQuery query, List<object> attributes)
        {
            String[] attributeNames = { "worldNo" };
            String[] attributeTypes = { "INT" };
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

            underlineDatabase.createNewTable(attributeTableName, attributeNames, attributeTypes);
        }

        private void createAttributeTables(SqlInsertQuery query, List<Object> attributes)
        {
            for (int i = 0; i < attributes.Count; i++)
            {
                string attributeTableName = query.TableName + "_" + i;
                string ai = "att" + i;
                String[] attributeNames = { "var", "v", ai , "p" };
                String[] attributeTypes = { "INT", "INT", "NVARCHAR(MAX)", "float" };
                underlineDatabase.createNewTable(attributeTableName, attributeNames, attributeTypes);
            }
        }


        public System.Data.DataTable viewTable(string tableName)
        {
            string sql = "Select * From " + tableName;
            return underlineDatabase.executeSQLWithResult(sql);
        }
    }
}
