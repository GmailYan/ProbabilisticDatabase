using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery
{
    class SqlSelectQuery
    {
        private string sql;
        private string _conditionClause;
        private string _tableName;
        private string _attributes;
        private SqlSelectQuery _subQuery;

        public SqlSelectQuery(string sql)
        {
            this.sql = sql;
        }

        public string TableName
        {
            get { return _tableName; }
        }

        public string ConditionClause
        {
            get { return _conditionClause; }
        }

        public string Attributes
        {
            get { return _attributes; }
        }

        internal void processAndPopulateEachField()
        {
            // pattern to match here is: select fields from tableORsubQuery where whereCondition 
            string sPattern = @"\s*SELECT\s+(?<attributes>.+)FROM\s+(?<tableClause>.+?)\s*(\z|WHERE\s+(?<conditionClause>.+))";
            Match match = Regex.Match(this.sql, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                _attributes = match.Groups["attributes"].Value;
                String tableClause = match.Groups["tableClause"].Value;
                _conditionClause = match.Groups["conditionClause"].Value;
               
                // TODO: do i really need to parse the attribute names ?
                List<String> attributesName = processAttributesClause(_attributes);
                processTableClause(tableClause, out _tableName, out _subQuery);

            }
            else
            {
                throw new Exception("query's format does not comply with INSERT INTO VALUES");
            }
        }

        /// <summary>
        /// at the moment only table name is allowed
        /// </summary>
        /// <param name="tableClause"></param>
        /// <param name="tableName"></param>
        /// <param name="subQuery"></param>
        private void processTableClause(string tableClause, out string tableName, out SqlSelectQuery subQuery)
        {
            // pattern here is: tableName or 2 table join or a sql select query all over again
            string sPattern = @"(?<tableName>\w+)";
            Match match = Regex.Match(tableClause, sPattern, RegexOptions.IgnoreCase);
            tableName = "";
            if(match.Success)
            {
                tableName = match.Groups["tableName"].Value;
            }
            
            subQuery = null;
        }

        private List<string> processAttributesClause(string attributes)
        {
            List<string> result = new List<string>();

            // pattern to match here is: attribute1,attribute2, .....
            string sPattern = @"(?<attributeName>\w+)";
            MatchCollection matchs = Regex.Matches(attributes, sPattern, RegexOptions.IgnoreCase);

            for (int i = 0; i < matchs.Count; i++)
            {
                string oneAttribute = matchs[i].Groups["attributeName"].Value;
                result.Add(oneAttribute);
            }

            return result;
        }
    }
}
