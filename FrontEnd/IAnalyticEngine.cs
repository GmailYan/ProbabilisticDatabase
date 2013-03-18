using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase
{
    interface IAnalyticEngine
    {
        ///<summary>
        ///receive a raw sql query from user through GUI layer
        ///submitting it to analyticEngine for refining and execution, the excution results is the return value.
        ///</summary>
        string submitSQL(string sql);

        string submitSQLWithResult(string sql, out DataTable answerSet);
        DataTable viewTable(string tableName);
    }
}
