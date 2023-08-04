package it.unitn.ds1;

//Utility class for pair element
public class Pair<T1,T2> {
    // T stands for "Type"
    private T1 a;

    private T2 b;

    Pair(T1 a,T2 b){
        this.a = a;
        this.b = b;

    }

    public T2 getValue() { return this.b;}
    public void setValue(T2 t) { this.b = t; }
    public T1 getKey() { return this.a;}
    public void setKey(T1 t) { this.a = t; }
}
