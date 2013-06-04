using System.Collections.Generic;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.Attribute
{
    public class ProbabilisticSingleAttribute : ProbabilisticAttribute
    {
        List<string> values;

        public List<string> Values
        {
            get { return values; }
            set { values = value; }
        }
        List<double> probs;

        public List<double> Probs
        {
            get { return probs; }
            set { probs = value; }
        }

        public ProbabilisticSingleAttribute(List<string> value,List<double> prob)
        {
            this.values = value;
            this.probs = prob;
        }

    }
}
