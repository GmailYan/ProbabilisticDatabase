using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    class OrderedSetIterator
    {
        const string tempTable = "OrderedSetOfWorldNo";
        private readonly List<List<Tuple<int,double>>> _worldNumbers;
        private int index = 0;
        private DataRowCollection _dataRows;
        private List<string> _tables;
        private readonly IStandardDatabase underlineDatabase;

        public OrderedSetIterator(List<string> tables, IStandardDatabase underlineDatabase)
        {
            _tables = tables;
            this.underlineDatabase = underlineDatabase;
            _worldNumbers = ReadWorldNumbers(tables);
            
            CreateOrderedSetOfWorldNoTable(_tables, _worldNumbers);
        }

        private void CreateOrderedSetOfWorldNoTable(List<string> tables, List<List<Tuple<int, double>>> worldNumbers)
        {
            underlineDatabase.DropTableIfExist(tempTable);
            var noOfTable = tables.Count;
            if(tables.Count != worldNumbers.Count)
            {
                throw new Exception("number of tables is inconsistent");
            }

            string selectClause = string.Format(" {0}_PossibleWorldsAggregated.worldNo as {0}", tables[0]);
            string fromClause = string.Format(" {0}_PossibleWorldsAggregated", tables[0]);
            string jointProbabilityClause = string.Format("{0}_PossibleWorldsAggregated.p/100", tables[0]); ;
            // SQL statement select worldNo from all relevent table and do a cross join
            for (int i = 1; i < noOfTable; i++)
            {
                var tName = tables[i];
                selectClause += string.Format(",{0}_PossibleWorldsAggregated.worldNo as {0}", tName);
                fromClause += string.Format(" cross join {0}_PossibleWorldsAggregated", tName);
                jointProbabilityClause += string.Format("*{0}_PossibleWorldsAggregated.p/100", tName);
            }
            selectClause += "," + jointProbabilityClause+"*100 as p ";
            var sql = string.Format("Select {0} INTO {2} From {1}", selectClause, fromClause, tempTable);
            underlineDatabase.ExecuteSql(sql);
            var result = underlineDatabase.ExecuteSqlWithResult("Select * FROM "+tempTable);
            _dataRows = result.Rows;
        }

        private List<List<Tuple<int, double>>> ReadWorldNumbers(List<string> tables)
        {
            var worldNumbers = new List<List<Tuple<int,double>>>();
            for (int i = 0; i < tables.Count; i++ )
            {
                var tName = tables[i];
                var sql = string.Format("Select worldNo,p from {0}_possibleWorldsAggregated",tName);
                var r = underlineDatabase.ExecuteSqlWithResult(sql);

                var worldNo = new List<Tuple<int, double>>();
                foreach (DataRow row in r.Rows)
                {
                    var w = row.Field<int>("worldNo");
                    var p = row.Field<double>("p");
                    Tuple<int, double> tuple = new Tuple<int, double>(w, p);
                    worldNo.Add(tuple);
                }
                worldNumbers.Add(worldNo);
            }
            return worldNumbers;
        }

        internal bool HasNext()
        {
            return index < _dataRows.Count;
        }

        internal Tuple<List<int>,double> NextSetOfWorldNo()
        {
            var worldNo = new List<int>();
            var p = new double();

            var row = _dataRows[index];

            p = row.Field<double>("p");
            for (int i = 0; i < _tables.Count; i++)
            {
                int w = row.Field<int>(_tables[i]);
                worldNo.Add(w);
            }
            var r = new Tuple<List<int>, double>(worldNo,p);
            index++;
            return r;
        }

        internal int GetIndex()
        {
            return index-1;
        }

    }
}
