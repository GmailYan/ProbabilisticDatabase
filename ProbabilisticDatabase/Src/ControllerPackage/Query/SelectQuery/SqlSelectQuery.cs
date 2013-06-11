using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery
{
    public class SqlSelectQuery
    {
        private string sql;
        private string _conditionClause = "";
        private string _strategyClause = "";
        private EvaluationStrategy _strategy;
        private string _tableName;
        private string _attributes;

        // Below fields are only related to subquery handling
        private SqlSelectQuery _subQuery;
        private bool _hasSubquery;
        private string _JoinOnAttributes;

        //------------------ Getter/Setter starts -------------------

        public string Sql
        {
            get { return sql; }
        }

        public SqlSelectQuery SubQuery
        {
            get { return _subQuery; }
        }
       
        public string JoinOnAttributes
        {
            get { return _JoinOnAttributes; }
        }

        public bool HasSubquery
        {
            get { return _hasSubquery; }
        }

        public string TableName
        {
            get { return _tableName; }
        }

        public string ConditionClause
        {
            get { return _conditionClause; }
        }
        public EvaluationStrategy Strategy
        {
            get { return _strategy; }
        }
        public string Attributes
        {
            get { return _attributes; }
        }
        //------------------ Getter/Setter ends -------------------

        public SqlSelectQuery(string sql)
        {
            this.sql = sql;
            processAndPopulateEachField();
            parseEvaluationStrategyEnum();
        }

        private void processAndPopulateEachField()
        {
            // pattern to match here is: select fields from tableORsubQuery where whereCondition EVALUATE USING xxx strategy
            string sPattern = @"\A\s*SELECT\s+(?<attributes>.+?)\s+FROM\s+(\[(?<tableClause>.+)\]|(?<tableName>\w+))\s*(?<conditionClause>.*)";
            Match match = Regex.Match(this.sql, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                _attributes = match.Groups["attributes"].Value;
                String tableClause = match.Groups["tableClause"].Value;
                String tableName = match.Groups["tableName"].Value;
                if (tableName.Length > 0)
                {
                    _tableName = tableName;
                    _subQuery = null;
                    _JoinOnAttributes = null;
                    _hasSubquery = false;
                }
                else
                {
                    processTableClause(tableClause, out _hasSubquery, out _tableName, out _subQuery, out _JoinOnAttributes);
                }
                var optionalClause = match.Groups["conditionClause"].Value;
               
                //List<String> attributesName = processAttributesClause(_attributes);

                if (optionalClause.Length > 0)
                   processOptionalClause(optionalClause, out _conditionClause, out _strategyClause);
            }
            else
            {
                throw new Exception("query's format does not comply with SELECT QUERY");
            }
        }

        private void parseEvaluationStrategyEnum()
        {
            switch (_strategyClause.ToLower()){
                case "monte carlo":
                    _strategy = EvaluationStrategy.MonteCarlo;
                    break;
                case "":
                    _strategy = EvaluationStrategy.Default;
                    break;
                default:
                    _strategy = EvaluationStrategy.Default;
                    break;
            }
        }

        /// <summary>
        /// Both WHERE clause and EVALUATE USING clause are optional
        ///
        /// </summary>
        /// <param name="optionalClause"></param>
        /// <param name="_conditionClause"></param>
        /// <param name="_strategyClause"></param>
        private void processOptionalClause(string optionalClause, out string _conditionClause, out string _strategyClause)
        {
            string sPattern = @".*WHERE\s*\((?<whereClause>.*?)\)";
            Match match = Regex.Match(optionalClause, sPattern, RegexOptions.IgnoreCase);

            _conditionClause = "";
            _strategyClause = "";

            if (match.Success)
            {
                _conditionClause = match.Groups["whereClause"].Value;
            }

            string sPattern2 = @".*EVALUATE USING\s*\((?<strategyClause>.*)\)";
            Match match2 = Regex.Match(optionalClause, sPattern2, RegexOptions.IgnoreCase);

            if (match2.Success)
            {
                _strategyClause = match2.Groups["strategyClause"].Value;
            }
        }

        /// <summary>
        /// at the moment only table name is allowed
        /// </summary>
        /// <param name="tableClause"></param>
        /// <param name="tableName"></param>
        /// <param name="subQuery"></param>
        private void processTableClause(string tableClause, out bool _hasSubquery, out string _tableName, out SqlSelectQuery _subQuery, out string _JoinOnAttributes)
        {
            // pattern here is: tableName or 2 table join with a sql select query all over again
            string sPattern = @"\s*(?<table1>\w+)\s*(\z|as\s+t1\s+join\s+\((?<table2>.*)\)\s+as\s+t2\s+on\s+(?<joinCondition>.+))";
            Match match = Regex.Match(tableClause, sPattern, RegexOptions.IgnoreCase);
            _tableName = "";
            if (match.Success)
            {
                _tableName = match.Groups["table1"].Value;
                var table2 = match.Groups["table2"].Value;

                if (table2 != null && table2.Length > 0)
                {
                    _hasSubquery = true;
                    _JoinOnAttributes = match.Groups["joinCondition"].Value;
                    SqlSelectQuery subQuery = new SqlSelectQuery(table2);
                    _subQuery = subQuery;
                }
                else
                {
                    _hasSubquery = false;
                    _subQuery = null;
                    _JoinOnAttributes = "";
                }
            }
            else
            {
                throw new Exception("parsing problem encountered within processTableClause function");
            }

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
