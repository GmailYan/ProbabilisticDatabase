using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using gudusoft.gsqlparser;
using gudusoft.gsqlparser.Units;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{
    public class GeneralSQLParser
    {
        public static void Parse(string sql)
        {
            TDbVendor db = TDbVendor.DbVMssql;
            TGSqlParser sqlparser = new TGSqlParser(db);
            sqlparser.Sqlfilename = sql;

            int i = sqlparser.Parse();

            if (i == 0)
            {
                foreach (TCustomSqlStatement stmt in sqlparser.SqlStatements)
                {
                    //stmt.SqlStatementType == TSqlStatementType.sstSelect; 
                    var psql = (TSelectSqlStatement)stmt;
                    // psql.SelectDistinct bool
                    //psql.SelectDistinctText string
                }
            }
            else
            {
                Console.WriteLine("Please make sure you are setting the correct database engine, current db engine is:" + db + Environment.NewLine + sqlparser.ErrorMessages);
            }
        }
    }
}
