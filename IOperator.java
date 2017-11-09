interface IOperator {
    public String setOperatorName(String name);
    public String getOperatorName();
    public String apply(Node node);
}