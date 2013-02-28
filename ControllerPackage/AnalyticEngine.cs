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

            SqlInsertQuery query = new SqlInsertQuery(sql);
            query.processAndPopulateEachField();

            bool isTableExist = underlineDatabase.checkIsTableAlreadyExist(query.TableName);

            if (!isTableExist)
            {
                List<Object> attributes = query.Attributes;
                String[] attributeNames = {"var","v","a1","p"};
                String[] attributeTypes = { "INT","INT","NVARCHAR(MAX)", "float" };
                int randomVariable = 1;

                for (int i = 0; i < attributes.Count; i++)
                {
                    string attributeTableName = query.TableName + "_" + i;
                    underlineDatabase.createNewTable(attributeTableName, attributeNames, attributeTypes);
                    if(attributes[i] is AttributeValue)
                    {
                        AttributeValue attribute = (AttributeValue)attributes[i];
                        double prob = 0;
                        if( i==0 )
                        {
                            prob = query.TupleP;
                        }else
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

                        for (int j = 0; j < v.Count; j++ )
                        {
                            // attribute value starting from 1 upto number of possible values, 
                            // because 0 is system reserve for null state.
                            underlineDatabase.insertValueIntoAttributeTable(attributeTableName, randomVariable, j+1, v[j], p[j]);
                        }
                    
                    }else{
                        // attribute is invalid type ?!
                    }
                }

            }
            else
            {
                // what to do ?
            }

            return "end of submitSQL function";
        }
    }
}
