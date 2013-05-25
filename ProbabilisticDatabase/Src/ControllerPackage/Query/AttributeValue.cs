using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{

    public class DeterministicAttribute
    {
        private string attributeValue;

        public string AttributeValue1
        {
            get { return attributeValue; }
            set { attributeValue = value; }
        }

        public DeterministicAttribute(string value)
        {
            attributeValue = value;
        }


    }
}
