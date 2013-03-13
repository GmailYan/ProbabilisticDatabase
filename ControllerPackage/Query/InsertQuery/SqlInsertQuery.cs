using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.InsertQuery
{
    public class SqlInsertQuery
    {
        private String sql;
        private String tableName;

        public String TableName
        {
            get { return tableName; }
            set { tableName = value; }
        }
        private List<Object> attributes = new List<Object>();

        public List<Object> Attributes
        {
            get { return attributes; }
            set { attributes = value; }
        }
        private double tupleP;

        public double TupleP
        {
            get { return tupleP; }
            set { tupleP = value; }
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
            string sPattern = @"INSERT\s+INTO\s+(?<tableName>\w+)\s+VALUES\s+(?<valueClause>\(.+\))\s*(?<tupleProbabilityClause>.*)";
            Match match = Regex.Match(this.sql, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                this.tableName = match.Groups["tableName"].Value;
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
            string sPattern = @"[(,]\s*(PROBABLY(?<pValue>[\w%\s/]+)|(?<value>\w+))";
            MatchCollection matchs = Regex.Matches(valueClause, sPattern, RegexOptions.IgnoreCase);
           
            if(matchs.Count == 0)
            {
                throw new Exception("regex matching fail to return any results");
            }

            for (int i=0; i<matchs.Count; i++ )
            {
                String value = matchs[i].Groups["value"].Value;
                String probabilisticValues = matchs[i].Groups["pValue"].Value;

                if (value != "" && probabilisticValues == "")
                {
                    AttributeValue pAttribute = new AttributeValue(value);
                    attributes.Add(pAttribute);
                }
                else
                {
                    if ( !(value == "" && probabilisticValues != ""))
                    {
                        throw new Exception("PROBABLY clause contain no values at all !");
                    }

                    ProbabilisticAttribute pAttribute = processProbabilisticValueClause(probabilisticValues);
                    attributes.Add(pAttribute);
                }

            }

        }

        public static ProbabilisticAttribute processProbabilisticValueClause(string probabilisticValues)
        {

            // pattern here is value 50% / value 25% etc
            string sPattern = @"\s*(?<value>\w+)\s+(?<prob>\d+)%\s*";
            MatchCollection matchs = Regex.Matches(probabilisticValues, sPattern, RegexOptions.IgnoreCase);

            if (matchs.Count == 0)
            {
                throw new Exception("no matching on Probabilistic Value Clause");
            }

            List<String> values = new List<String>();
            List<Double> pValues = new List<Double>();
            for (int i = 0; i < matchs.Count; i++)
            {
                String value = matchs[i].Groups["value"].Value;
                String probability = matchs[i].Groups["prob"].Value;
                double attributeP = 0;
                bool isSuccess = Double.TryParse(probability, out attributeP);

                if (!isSuccess)
                {
                    throw new Exception("probability value invalid");
                }

                values.Add(value);
                pValues.Add(attributeP);
            }
            ProbabilisticAttribute pAttribute = new ProbabilisticAttribute(values, pValues);
            return pAttribute;
            
        }


    }
}
