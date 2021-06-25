package com.redshoes.metamodel.schema;



/**
 * jdbc 字段长度
 *
 * @author leiyi
 */
public class ColumnSize {

    public static final ColumnSize EMPTY = new ColumnSize();
    /**
     * 总长度
     */
    private Integer totalSize;
    /**
     * 小数位
     */
    private Integer decimalSize;



    private ColumnSize(){}

    private ColumnSize(Integer totalSize, Integer decimalSize){
        if(totalSize != null && totalSize <= 0){
            throw new IllegalArgumentException("totalSize can not less 0 ");
        }
        setTotalSize(totalSize);
        setDecimalSize(decimalSize);
    }

    private ColumnSize(Integer totalSize){
        setTotalSize(totalSize);
    }

    public static ColumnSize of(Integer totalSize,Integer decimalSize){
        return new ColumnSize(totalSize,decimalSize);
    }

    public static ColumnSize of(Integer totalSize){
        return new ColumnSize(totalSize);
    }

    public Integer getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(Integer totalSize) {
        this.totalSize = totalSize;
    }

    public Integer getDecimalSize() {
        return decimalSize;
    }

    public void setDecimalSize(Integer decimalSize) {
        this.decimalSize = decimalSize;
    }

    /**
     * 是否有小数位
     * @return
     */
    public boolean isDecimal(){
        if(getDecimalSize() == null){
            return false;
        }
        return true;
    }

    /**
     * 是否为空
     * @return
     */
    public boolean isEmpty(){
        if(getTotalSize() == null && getDecimalSize() == null){
            return true;
        }
        return false;
    }
    /**
     * 获得字段长度表达式 eg. decimal(38,19)
     * @return
     */
    public String getColumnSizeContent() {
        if(getTotalSize() == null){
            return null;
        }
        StringBuilder sizeContent = new StringBuilder();
        sizeContent.append(getTotalSize().intValue());
        if (getDecimalSize() != null) {
            sizeContent.append(",");
            sizeContent.append(getDecimalSize().intValue());
        }
        return sizeContent.toString();
    }

}
