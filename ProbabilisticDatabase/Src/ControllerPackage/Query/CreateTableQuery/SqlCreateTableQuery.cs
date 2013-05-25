using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.CreateTableQuery
{
    public class SqlCreateTableQuery
    {
        private string _sql;
        private string _tableName;

        public string TableName
        {
            get { return _tableName; }
        }
        private List<String> _attributeNames;

        public List<String> AttributeNames
        {
            get { return _attributeNames; }
        }
        private List<String> _attributeTypes;

        public List<String> AttributeTypes
        {
            get { return _attributeTypes; }
        }

        public SqlCreateTableQuery(string sql)
        {
            this._sql = sql;
            processAndPopulateEachField();
        }

        private void processAndPopulateEachField()
        {
            // pattern here is CREATE TABLE tablename ( columnName columnType, ... repeat )
            string sPattern = @"CREATE\s+TABLE\s+(?<tableName>\w+)\s+\((?<valueClause>.+)\)";
            Match match = Regex.Match(this._sql, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                _tableName = match.Groups["tableName"].Value;
                var attributeClause = match.Groups["valueClause"].Value;
                processAttributeValue(attributeClause,out _attributeNames,out _attributeTypes);
            }
            else
            {
                _tableName = "";

            }
            

        }

        private void processAttributeValue(string attributeClause, out List<string> _attributeNames, out List<string> _attributeTypes)
        {
            // pattern here is  columnName columnType, ... repeat 
            string sPattern = @"(?<attributeName>\w+)\s+(?<attributeType>.+?)\s*(,|\z)";
            MatchCollection matches = Regex.Matches(attributeClause, sPattern, RegexOptions.IgnoreCase);
            _attributeNames = new List<string>();
            _attributeTypes = new List<string>();

            for (int i = 0; i < matches.Count; i++)
            {
                var current = matches[i];
                var name = current.Groups["attributeName"].Value;
                var type = current.Groups["attributeType"].Value;
                _attributeNames.Add(name);
                _attributeTypes.Add(type);
            }

        }




    }
}
