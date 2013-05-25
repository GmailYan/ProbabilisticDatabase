using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    public class CreateTableHandler
    {
        private Query.CreateTableQuery.SqlCreateTableQuery cquery;
        private IStandardDatabase underlineDatabase;

        public CreateTableHandler(Query.CreateTableQuery.SqlCreateTableQuery cquery, IStandardDatabase underlineDatabase)
        {
            this.cquery = cquery;
            this.underlineDatabase = underlineDatabase;
        }

        internal System.Data.DataTable HandleCreateTableQuery()
        {
            createAttributeTables(cquery.TableName,cquery.AttributeNames,cquery.AttributeTypes,underlineDatabase);
            createPossibleStatesTable(cquery.TableName, cquery.AttributeNames, cquery.AttributeTypes, underlineDatabase);
            createPossibleWorldsTable(cquery.TableName, cquery.AttributeNames, cquery.AttributeTypes, underlineDatabase);
            return null;
        }



        private void createPossibleStatesTable(string tableName, List<string> attNames, List<string> attTypes, IStandardDatabase underlineDatabase)
        {
            String[] attributeNames = { "var", "v" };
            String[] attributeTypes = { "INT", "INT" };
            string attributeTableName = tableName + "_PossibleStates";

            List<String> attributeNamesList = attributeNames.ToList();
            List<String> attributeTypesList = attributeTypes.ToList();

            attributeNamesList.AddRange(attNames);
            attributeTypesList.AddRange(attTypes);

            // p is the last attribute
            attributeNamesList.Add("p");
            attributeTypesList.Add("float");

            underlineDatabase.CreateNewTable(attributeTableName, attributeNamesList.ToArray(), attributeTypesList.ToArray());
        }

        private void createPossibleWorldsTable(string tableName, List<string> attNames, List<string> attTypes, IStandardDatabase underlineDatabase)
        {
            String[] attributeNames = { "worldNo" };
            String[] attributeTypes = { "INT" };
            string attributeTableName = tableName + "_PossibleWorlds";

            List<String> attributeNamesList = attributeNames.ToList();
            List<String> attributeTypesList = attributeTypes.ToList();

            attributeNamesList.AddRange(attNames);
            attributeTypesList.AddRange(attTypes);

            // p is the last attribute
            attributeNamesList.Add("p");
            attributeTypesList.Add("float");

            underlineDatabase.CreateNewTable(attributeTableName, attributeNamesList.ToArray(), attributeTypesList.ToArray());
        }

        private void createAttributeTables(string tableName, List<string> attNames, List<string> attTypes, IStandardDatabase underlineDatabase)
        {
            String[] attributeNames1 = { "var", "v", "TupleExistence", "p" };
            String[] attributeTypes1 = { "INT", "INT", "NVARCHAR(MAX)", "float" };
            underlineDatabase.CreateNewTable(tableName + "_0", attributeNames1, attributeTypes1);

            for (int i = 1; i <= attNames.Count; i++)
            {
                string attributeTableName = tableName + "_" + i;
                String[] attributeNames = { "var", "v", attNames[i-1], "p" };
                String[] attributeTypes = { "INT", "INT", attTypes[i-1], "float" };
                underlineDatabase.CreateNewTable(attributeTableName, attributeNames, attributeTypes);
            }
        }


    }
}
