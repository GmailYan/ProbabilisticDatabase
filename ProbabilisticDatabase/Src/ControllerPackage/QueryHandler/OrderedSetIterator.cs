using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    class OrderedSetIterator
    {
        private List<List<Tuple<int,double>>> worldNumbers;
        private int index = 1;
        private List<string> tables;
        private IStandardDatabase underlineDatabase;

        public OrderedSetIterator(List<string> tables, IStandardDatabase underlineDatabase)
        {
            this.tables = tables;
            this.underlineDatabase = underlineDatabase;
            worldNumbers = ReadWorldNumbers(tables);
        }

        private List<List<Tuple<int, double>>> ReadWorldNumbers(List<string> tables)
        {
            List<List<Tuple<int, double>>> worldNumbers = new List<List<Tuple<int,double>>>();
            for (int i = 0; i < tables.Count; i++ )
            {
                var tName = tables[i];
                var sql = string.Format("Select worldNo,p from {0}_possibleWorlds",tName);
                var r = underlineDatabase.ExecuteSqlWithResult(sql);

            }
            return worldNumbers;
        }

        internal bool hasNext()
        {
            return index <= worldNumbers.Count;
        }

        internal List<int> nextSetOfWorldNo()
        {
            var tuple = worldNumbers[index];
            var result = from r in tuple select r.Item1; 
            return result.ToList();
        }

        internal int GetIndex()
        {
            return index;
        }

        internal double GetJointProbability()
        {
            var tuple = worldNumbers[index];
            var result = from r in tuple select r.Item2;
            result.ToList().Join();
            return 1;
        }
    }
}
