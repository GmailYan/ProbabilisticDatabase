using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery
{
    /// <summary>
    /// This class might store the extra information only available to Engine in runtime
    /// in contrast to those static information SqlSelectQuery stored.
    /// </summary>
    public class SelectQuery
    {
        private SqlSelectQuery query;
        private IStandardDatabase underlineDatabase;

        public SelectQuery(SqlSelectQuery query, IStandardDatabase underlineDatabase)
        {
            this.query = query;
            this.underlineDatabase = underlineDatabase;
            readTableAndGetAttributeTypes();
        }

        private void readTableAndGetAttributeTypes()
        {
            var r = underlineDatabase.ExecuteSqlWithResult("SELECT * FROM metaTable WHERE tableName="+query.TableName);
            if (r.Rows.Count != 1)
                throw new Exception("metaTable invalid");

            var row = r.Rows[0];
            
        }

        public object getAttributes()
        {
            throw new NotImplementedException();
        }

        public object getAttributeTypes()
        {
            throw new NotImplementedException();
        }
    }

}
