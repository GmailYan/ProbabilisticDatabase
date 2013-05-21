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
        private string sql;
        private string tableName;

        public SqlCreateTableQuery(string sql)
        {
            this.sql = sql;
            processAndPopulateEachField();
        }

        internal void processAndPopulateEachField()
        {
            // pattern here is CREATE TABLE tablename ( columnName columnType, ... repeat )
            string sPattern = @"CREATE\s+TABLE\s+(?<tableName>\w+)\s+((?<valueClause>.*))";
            Match match = Regex.Match(this.sql, sPattern, RegexOptions.IgnoreCase);
            //match.Captures.Equals

        }



    }
}
