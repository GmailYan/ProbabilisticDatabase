using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{
    class SqlQuery
    {
        private string sql;

        public SqlQuery(string sql)
        {
            this.sql = sql;
        }

        /// <summary>
        /// basic class is used to classify which sql query received.
        /// at the moment only insert, select sql is supported
        /// </summary>
        /// <returns>If error return null ?!</returns>
        public QueryType processType()
        {
            string sPattern = @"\A\s*(?<queryType>\w+)";
            Match match = Regex.Match(this.sql, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                string rawType = match.Groups["queryType"].Value;

                switch (rawType.ToLower())
                {
                    case "insert":
                        return QueryType.INSERT;
                    case "select":
                        return QueryType.SELECT;
                    default:
                        return QueryType.INVALID;
                }

            }
            else
            {
                // match fail, query invalid
                return QueryType.INVALID;
            }

        }
    }
}
