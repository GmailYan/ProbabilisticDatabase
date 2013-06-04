namespace ProbabilisticDatabase.Src.ControllerPackage.Query.Attribute
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
