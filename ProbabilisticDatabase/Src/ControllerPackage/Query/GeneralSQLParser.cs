using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Borland.Vcl;
using gudusoft.gsqlparser;
using gudusoft.gsqlparser.Units;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{
    public class GeneralSQLParser
    {
        private readonly TGSqlParser sqlparser;

        public GeneralSQLParser()
        {
            TDbVendor db = TDbVendor.DbVMssql;
            sqlparser = new TGSqlParser(db);
        }

        public void Parse(string sql)
        {
            sqlparser.SqlText.Add(sql);

            int i = sqlparser.Parse();

            if (i == 0)
            {
                foreach (TCustomSqlStatement stmt in sqlparser.SqlStatements)
                {
                    AnalyzeStmt(stmt);
                }
            }
            else
            {
                Console.WriteLine(
                    "Please make sure you are setting the correct database engine, current db engine is:" + sqlparser.DbVendor +
                    Environment.NewLine + sqlparser.ErrorMessages);
            }
        }

        private void AnalyzeStmt(TCustomSqlStatement psql)
        {
            switch (psql.SqlStatementType)
            {
                case TSqlStatementType.sstSelect:
                    AnalyzeSelectStmt((TSelectSqlStatement) psql);
                    break;
                default:
                    Console.WriteLine(psql.SqlStatementType.ToString());
                    break;
            }
        }

        private void AnalyzeSelectStmt(TSelectSqlStatement pSqlstmt)
        {
            SqlSelect q = new SqlSelect();
            // where clause this query is rule Select
            if (pSqlstmt.WhereClause != null)
            {
                Console.WriteLine(pSqlstmt.WhereClauseText);
                q.WhereClause = pSqlstmt.WhereClauseText;
            }

            //column here, when column != *, indep project
            List<string> names = new List<string>();
            foreach (TLzField fld in pSqlstmt.Fields)
            {
                var lcstr ="\n\tFullname:" + fld.FieldFullname;
                lcstr = lcstr + "\n\tPrefix:" + fld.FieldPrefix;
                lcstr = lcstr + "\tColumn:" + fld.FieldName;
                lcstr = lcstr + "\talias:" + fld.FieldAlias;
                Console.WriteLine(lcstr);
                names.Add(fld.FieldName);
            }
            q.SelectClause = names;

            // join
            names = new List<string>();
            foreach (var table in pSqlstmt.JoinTables)
            {
                Console.WriteLine(table.JoinTable.TableName);
                Console.WriteLine(table.JoinItems.Count());
                names.Add(table.JoinTable.TableName);
            }
            q.SelectClause = names;


            switch (pSqlstmt.SelectSetType)
            {
                case TSelectSetType.sltNone:
                    break;
                case TSelectSetType.sltUnion:
                    // indep union rule
                    Console.WriteLine(pSqlstmt.SelectClauseText);
                    break;
                case TSelectSetType.sltUnionAll:
                    break;
                case TSelectSetType.sltMinus:
                    break;
                case TSelectSetType.sltIntersect:
                    break;
                case TSelectSetType.sltIntersectAll:
                    break;
                case TSelectSetType.sltExcept:
                    // indep negation rule
                    Console.WriteLine(pSqlstmt.FromClauseText);
                    break;
                case TSelectSetType.sltExceptAll:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private class SqlSelect
        {
            private string _whereClause;
            private List<string> selectFields; 

            public string WhereClause
            {
                get { return _whereClause; }
                set { _whereClause = WhereClause; }
            }

            public List<string> SelectClause
            {
                get { return selectFields; }
                set { selectFields = SelectClause; }
            }
        }
    }
}
