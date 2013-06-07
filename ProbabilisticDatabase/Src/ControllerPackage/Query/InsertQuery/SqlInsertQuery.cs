using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using ProbabilisticDatabase.Src.ControllerPackage.Query.Attribute;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.InsertQuery
{
    public class SqlInsertQuery
    {
        private String sql;
        private String _tableName;
        private List<String> _colNames;
 
        public String TableName
        {
            get { return _tableName; }
            set { _tableName = value; }
        }
        private List<Object> _attributes = new List<Object>();

        public List<Object> Attributes
        {
            get { return _attributes; }
            set { _attributes = value; }
        }
        private double tupleP;

        public double TupleP
        {
            get { return tupleP; }
            set { tupleP = value; }
        }

        public List<string> ColNames
        {
            get { return _colNames; }
        }

        /// <summary>
        /// this constructor is just storing the sql, calling processAndPopulateEachField() to parse it 
        /// before access underlined data 
        /// </summary>
        /// <param name="sql"></param>
        public SqlInsertQuery(string sql)
        {
            this.sql = sql;

            //initialise tuple to be non-exist
            this.tupleP = 0;
        }

        public void processAndPopulateEachField()
        {
            // pattern here is INSERT INTO tableName VALUES (valueClause) PROBABLY pvalue
            string sPattern = @"INSERT\s+INTO\s+(?<tableName>\w+)\s*(\s*|(?<colNames>\(.*?\)))\s+VALUES\s+(?<valueClause>\(.+\))\s*(?<tupleProbabilityClause>.*)";
            Match match = Regex.Match(sql, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                _tableName = match.Groups["tableName"].Value;
                string colNames = match.Groups["colNames"].Value;
                if (!string.IsNullOrEmpty(colNames))
                {
                    _colNames = ProcessColumnNames(colNames);
                }

                String valueClause = match.Groups["valueClause"].Value;
                String tupleProbabilityClause = match.Groups["tupleProbabilityClause"].Value;

                processValueClause(valueClause);
                processTupleProbabilityClause(tupleProbabilityClause);
            }
            else
            {
                throw new Exception("query's format does not comply with INSERT INTO VALUES");
            }

        }

        private List<string> ProcessColumnNames(string colNames)
        {
            List<String> result = new List<string>();

            string sPattern = @"[\(\,]\s*(?<name>\w+?)\s*(?=[\)\,])";
            MatchCollection matchs = Regex.Matches(colNames, sPattern, RegexOptions.IgnoreCase);
            if(matchs.Count == 0)
            {
                throw new Exception("regex matching fail to return any results");
            }

            for (int i = 0; i < matchs.Count; i++)
            {
                string name = matchs[i].Groups["name"].Value;
                result.Add(name);
            }
            return result;
        }

        private void processTupleProbabilityClause(string tupleProbabilityClause)
        {
            // pattern here is PROBABLY 50% or empty
            string sPattern = @"PROBABLY\s+(?<percentage>\d+)%";
            Match match = Regex.Match(tupleProbabilityClause, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                string valueClause = match.Groups["percentage"].Value;
                bool isSuccess = Double.TryParse(valueClause, out this.tupleP);

                if (!isSuccess)
                {
                    throw new Exception("probability value invalid");
                }
            }
            else
            {
                this.tupleP = 100.0;
            }

        }

        private void processValueClause(string valueClause)
        {
            // pattern here is value or PROBABLY value 50% and repeat for any length 
            string sPattern = @"[(,]\s*(PROBABLY(?<pValue>[\[\]\w%\.\s/]+)|(?<value>[\w\.]+))";
            MatchCollection matchs = Regex.Matches(valueClause, sPattern, RegexOptions.IgnoreCase);
            if(matchs.Count == 0)
            {
                throw new Exception("regex matching fail to return any results");
            }

            for (int i=0; i<matchs.Count; i++ )
            {
                String value = matchs[i].Groups["value"].Value;
                String probabilisticValues = matchs[i].Groups["pValue"].Value;

                if ( !String.IsNullOrEmpty(value) && String.IsNullOrEmpty(probabilisticValues))
                {
                    DeterministicAttribute pAttribute = new DeterministicAttribute(value);
                    _attributes.Add(pAttribute);
                }
                else
                {
                    if (!String.IsNullOrEmpty(value) || String.IsNullOrEmpty(probabilisticValues) )
                    {
                        throw new Exception("PROBABLY clause contain no values at all !");
                    }

                    ProbabilisticAttribute probabilisticAttribute = processProbabilisticValueClause(probabilisticValues);
                    _attributes.Add(probabilisticAttribute);
                }

            }

        }

        public static ProbabilisticAttribute processProbabilisticValueClause(string probabilisticValues)
        {

            // pattern here is: Word DecimalNumber% / Word DecimalNumber% / ... (repeats)
            string sPattern = @"\s*((?<value>[\w\s\.]+)|(?<values>\[.+?\]))\s+(?<prob>\d+)%\s*";
            MatchCollection matchs = Regex.Matches(probabilisticValues, sPattern, RegexOptions.IgnoreCase);

            if (matchs.Count == 0)
            {
                throw new Exception("no matching on Probabilistic Value Clause");
            }

            List<String> values = new List<String>();
            List<Double> pValues = new List<Double>();
            List<List<string>> multiAttrbutes = new List<List<string>>();
            for (int i = 0; i < matchs.Count; i++)
            {
                String value = matchs[i].Groups["value"].Value;
                String valuesCapture = matchs[i].Groups["values"].Value;

                String probability = matchs[i].Groups["prob"].Value;
                double attributeP = 0;
                bool isSuccess = Double.TryParse(probability, out attributeP);
                if (!isSuccess)
                {
                    throw new Exception("probability value invalid");
                }
                pValues.Add(attributeP);

                if (!string.IsNullOrEmpty(valuesCapture))
                {
                    List<String> ListOfValue = ProcessListOfValues(valuesCapture);
                    multiAttrbutes.Add(ListOfValue);
                }
                else if(!string.IsNullOrEmpty(value))
                {
                    values.Add(value);
                }
                else
                {
                    throw new Exception("both value and values are empty !");
                }

            }

            if(values.Count > 0 && multiAttrbutes.Count==0)
            {
                var pSingleAttribute = new ProbabilisticSingleAttribute(values, pValues);
                return pSingleAttribute;    
            }

            if (multiAttrbutes.Count > 0 && values.Count == 0)
            {
                bool consistency = true;
                int multiAttributesSize = multiAttrbutes[0].Count;
                foreach (var multiAttrbute in multiAttrbutes)
                {
                    if (multiAttributesSize != multiAttrbute.Count)
                    {
                        // multi attributes sould be of the same size.
                        consistency = false;
                    }
                }

                if (consistency)
                {
                    var multiAttribute = new ProbabilisticMultiAttribute(multiAttrbutes, pValues);
                    return multiAttribute;
                }
            }
            throw new Exception("fail to parse probabilistic multi attributes");
        }

        private static List<string> ProcessListOfValues(string valuesCapture)
        {
            List<string> result = new List<string>();
            string sPattern = @"[\[\s]\s*(?<value>\w+?)\s*(?=([\]\s]))";

            MatchCollection matchs = Regex.Matches(valuesCapture, sPattern, RegexOptions.IgnoreCase);
            for (int i = 0; i < matchs.Count; i++)
            {
                var oneValue = matchs[i].Groups["value"].Value;
                result.Add(oneValue);
            }
            return result;
        }
    }
}
