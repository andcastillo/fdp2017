interface IOperator {
    public void setOperatorName(String name);
    public String getOperatorName();
    public String apply(Node node);
}