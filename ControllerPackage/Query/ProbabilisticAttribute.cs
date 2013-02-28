using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{
    public class ProbabilisticAttribute
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

        public ProbabilisticAttribute(List<string> value,List<double> prob)
        {
            this.values = value;
            this.probs = prob;
        }

    }
}
