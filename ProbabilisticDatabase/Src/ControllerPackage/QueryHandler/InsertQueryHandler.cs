using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;
using ProbabilisticDatabase.Src.ControllerPackage.Query.Attribute;
using ProbabilisticDatabase.Src.ControllerPackage.Query.InsertQuery;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    public class InsertQueryHandler
    {
        private SqlInsertQuery query;
        private IStandardDatabase underlineDatabase;

        public InsertQueryHandler(SqlInsertQuery query, IStandardDatabase underlineDatabase)
        {
            this.query = query;
            this.underlineDatabase = underlineDatabase;
        }

        /// <summary>
        /// its operation is described by google doc chapter 1 storage, 
        /// if table already exist then ignore the create table operation
        /// just do the insert value
        /// </summary>
        /// <param name="query"></param>
        public DataTable HandleInsertQuery()
        {
            List<object> attributes = query.Attributes;
            var tableName = query.TableName;

            int randomVariable = 1;
            List<string> tNames = GetAttributeTableNames(attributes, query.ColNames, tableName);

            // table creation operation here
            CreateAttributeTables(attributes, tableName, query.ColNames, tNames);
            
            randomVariable = underlineDatabase.GetNextFreeVariableId(query.TableName);
            if (randomVariable <= 0)
            {
                randomVariable = 1;
            }

            // insert value into tables starting here
            InsertAttributeValue(query.TableName, attributes, randomVariable, tNames);

            return null;
        }

        private void InsertAttributeValue(string tableName, List<object> attributes, int randomVariable, List<string> tNames)
        {
            var tableJoins = string.Join(",", tNames);
            underlineDatabase.InsertValueIntoAttributeTable(tableName + "_0", randomVariable, 1, tableJoins, query.TupleP);

            for (int i = 1; i <= attributes.Count; i++)
            {
                var attObj = attributes[i - 1];
                if (attObj is DeterministicAttribute)
                {
                    var attribute = attObj as DeterministicAttribute;
                    const double prob = 100;

                    underlineDatabase.InsertValueIntoAttributeTable(tNames[i-1], randomVariable, 1, attribute.AttributeValue1, prob);
                }
                else if (attObj is ProbabilisticSingleAttribute)
                {
                    var attribute = attObj as ProbabilisticSingleAttribute;
                    List<String> v = attribute.Values;
                    List<double> p = attribute.Probs;

                    for (int j = 0; j < v.Count; j++)
                    {
                        // attribute value starting from 1 upto number of possible values, 
                        // because 0 is system reserve for null state.
                        underlineDatabase.InsertValueIntoAttributeTable(tNames[i - 1], randomVariable, j + 1, v[j], p[j]);
                    }
                }
                else if (attObj is ProbabilisticMultiAttribute)
                {
                    var pAttributes = attObj as ProbabilisticMultiAttribute;
                    var vs = pAttributes.MultiAttrbutes;
                    var p = pAttributes.PValues;

                    for (int j = 0; j < vs.Count; j++)
                    {
                        underlineDatabase.InsertValueIntoAttributeTable(tNames[i - 1], randomVariable, j + 1, vs[j], p[j]);
                    }

                }
                else
                {
                    throw new Exception("attribute object invalid");
                }

            }
        }

        private List<string> GetAttributeTableNames(List<object> attributes, List<string> colNames, string tableName)
        {
            bool colNameSpecified = colNames != null && colNames.Count > 0;
            var nameList = new List<string>();
            int colIndex = 0;
            for (int i = 0; i < attributes.Count; i++ )
            {
                string tName;
                var attObj = attributes[i];
                if(attObj is DeterministicAttribute)
                {
                    tName = colNameSpecified ? tableName + "_" + ConvertAttributeNameToIndex(colNames[colIndex]) 
                        : tableName + "_" + (colIndex + 1);
                    nameList.Add(tName);
                    colIndex++;
                }else if(attObj is ProbabilisticSingleAttribute)
                {
                    tName = colNameSpecified ? tableName + "_" + ConvertAttributeNameToIndex(colNames[colIndex]) 
                        : tableName + "_" + (colIndex + 1);
                    nameList.Add(tName);
                    colIndex++;
                }else if(attObj is ProbabilisticMultiAttribute)
                {
                    var multi = attObj as ProbabilisticMultiAttribute;
                    var colSize = multi.MultiAttrbutes[0].Count;
                    tName = tableName+"_";
                    if(colNameSpecified)
                    {
                        for (int j = 0; j < colSize; j++)
                        {
                            tName += ConvertAttributeNameToIndex(colNames[colIndex]);
                            colIndex++;
                        }
                    }
                    else
                    {
                        for (int j = 0; j < colSize; j++)
                        {
                            tName += (colIndex + 1);
                            colIndex++;
                        }
                    }
                    nameList.Add(tName);
                }
            }
            return nameList;
        }

        private int ConvertAttributeNameToIndex(string index)
        {
            string sPattern = @"att(?<number>\d)";
            Match match = Regex.Match(index, sPattern, RegexOptions.IgnoreCase);
            if (match.Success)
            {
                var stringValue = match.Groups["number"].Value;
                return int.Parse(stringValue);
            }
            return 0;
        }

        private void CreateAttributeTables(List<object> attributes, string tableName, List<string> columnNames, List<string> tNames)
        {
            bool colNameSpecified = columnNames != null && columnNames.Count>0;
            String[] attributeNames1 = { "var", "v", "att0", "p" };
            String[] attributeTypes1 = { "INT", "INT", "NVARCHAR(MAX)", "float" };
            underlineDatabase.CreateNewTable(tableName + "_0", attributeNames1, attributeTypes1);
            int colIndex = 0;
            for (int i = 0; i < attributes.Count; i++)
            {
                var attributeObject = attributes[i];
                if( attributeObject is DeterministicAttribute)
                {
                    var attIndex = colNameSpecified? ConvertAttributeNameToIndex(columnNames[colIndex]) : colIndex+1;

                    String[] attributeNames = { "var", "v", "att" + attIndex, "p" };
                    String[]  attributeTypes = { "INT", "INT", "NVARCHAR(MAX)", "float" };
                    underlineDatabase.CreateNewTable(tNames[i], attributeNames, attributeTypes);
                    colIndex++;
                }
                else if (attributeObject is ProbabilisticSingleAttribute)
                {
                    var attIndex = colNameSpecified ? ConvertAttributeNameToIndex(columnNames[colIndex]) : colIndex + 1;

                    String[] attributeNames = { "var", "v", "att" + attIndex, "p" };
                    String[] attributeTypes = { "INT", "INT", "NVARCHAR(MAX)", "float" };
                    underlineDatabase.CreateNewTable(tNames[i], attributeNames, attributeTypes);
                    colIndex++;
                }
                else if (attributeObject is ProbabilisticMultiAttribute)
                {
                    var multiAttribute = (ProbabilisticMultiAttribute)attributeObject;
                    var noOfAtt = multiAttribute.MultiAttrbutes.Count;
                    var attributeNames = new List<string>(){ "var", "v" };
                    var attributeTypes = new List<string>(){"INT", "INT"};
                    for (int j = 0; j < noOfAtt; j++)
                    {
                        var cName = colNameSpecified? columnNames[colIndex+j] : "att"+(colIndex+j+1);
                        attributeNames.Add(cName);
                        attributeTypes.Add("NVARCHAR(MAX)");
                    }

                    attributeNames.Add("p");
                    attributeTypes.Add("float");
                    underlineDatabase.CreateNewTable(tNames[i], attributeNames.ToArray(), attributeTypes.ToArray());
                    colIndex += noOfAtt;
                }
                else
                {
                    throw new Exception("attribute object invalid");
                }
            }
        }
    }
}