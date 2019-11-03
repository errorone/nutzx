package org.nutz.dao;

import org.nutz.dao.entity.Entity;
import org.nutz.dao.entity.MappingField;
import org.nutz.dao.impl.SimpleNesting;
import org.nutz.dao.jdbc.ValueAdaptor;
import org.nutz.dao.pager.Pager;
import org.nutz.dao.sql.Criteria;
import org.nutz.dao.sql.GroupBy;
import org.nutz.dao.sql.OrderBy;
import org.nutz.dao.sql.Pojo;
import org.nutz.dao.util.Daos;
import org.nutz.dao.util.cnd.SimpleCondition;
import org.nutz.dao.util.cri.*;
import org.nutz.lang.Lang;
import org.nutz.lang.Strings;
import org.nutz.lang.segment.CharSegment;
import org.nutz.lang.util.Callback2;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Locale;
import java.util.function.Function;

/* 原有的 Cnd 已经实现所有功能，为了能使开发过程中修改 Bean 对象编译时发现错误
 * @author errorone
 * @see Condition
 */
public class Cndx<T> implements OrderBy, Criteria, GroupBy {

    private static SerializedLambda resolve(Serializable fn){
        //先检查缓存中是否已存在
        SerializedLambda lambda = null;
        try{//提取SerializedLambda并缓存
            Method method = fn.getClass().getDeclaredMethod("writeReplace");
            method.setAccessible(Boolean.TRUE);
            lambda = (SerializedLambda) method.invoke(fn);
        }
        catch (Exception e){
            new DaoException(e.getMessage());
        }
        return lambda;
    }


    /**
     * 根据 Get方法名称获取变量名称
     * @param name
     * @return
     */
    public static String methodToProperty(String name) {
        if (name.startsWith("is")) {
            name = name.substring(2);
        } else {
            if (!name.startsWith("get") && !name.startsWith("set")) {
                throw new DaoException("Error parsing property name '" + name + "'.  Didn't start with 'is', 'get' or 'set'.");
            }
            name = name.substring(3);
        }
        if (name.length() == 1 || name.length() > 1 && !Character.isUpperCase(name.charAt(1))) {
            name = name.substring(0, 1).toLowerCase(Locale.ENGLISH) + name.substring(1);
        }
        return name;
    }
    public interface CName<T, R> extends Function<T, R>, Serializable {
    }


    /**
     * 根据 Lambda Get方法获取变量名称
     * @param cName
     * @return
     */
    public static <T> String getColumnName(CName<T,?> cName) {
        SerializedLambda sl = Cndx.resolve(cName);
        return Cndx.methodToProperty(sl.getImplMethodName());
    }

    public static <T> Cndx<T> where(CName<T,?> cName, SqlKeyword op, Object value) {
        return new Cndx(Cndx.exp(cName, op, value));
    }
    public static <T> SqlExpression exp(CName<T,?> cName, SqlKeyword op, Object value) {
        String name = getColumnName(cName);
        if(value!=null && value instanceof Nesting){
            return NestExps.create(name, op.toString(), (Nesting) value);
        }
        return Exps.create(name, op.toString(), value);
    }
    public static <T> SqlExpression expEX(CName<T,?> cName, SqlKeyword op, Object value) {
        if (_ex(value))
            return null;
        return Cndx.exp(cName, op, value);
    }
    public static <T> SqlExpressionGroup exps(CName<T,?> cName, SqlKeyword op, Object value) {
        String name = getColumnName(cName);
        return exps(exp(name,  op.toString(), value));
    }


    /**
     * @return 一个 Cnd 的实例
     */

    public static <T> Cndx<T> me() {
        return new Cndx<T>();
    }


    public OrderBy asc(CName<T,?> cName) {
        cri.asc(getColumnName(cName));
        return this;
    }

    public OrderBy desc(CName<T,?> cName) {
        cri.desc(getColumnName(cName));
        return this;
    }

    public Cndx or(CName<T,?> cName, SqlKeyword op, Object value) {
        return or(Cndx.exp(cName, op, value));
    }
    public Cndx and(CName<T,?> cName, SqlKeyword op, Object value) {
        return and(Cndx.exp(cName, op, value));
    }
    public Cndx andNot(CName<T,?> cName, SqlKeyword op, Object value) {
        return andNot(Cndx.exp(cName, op, value));
    }
    public Cndx orNot(CName<T,?> cName, SqlKeyword op, Object value) {
        return orNot(Cndx.exp(cName, op, value));
    }
    public Cndx andEX(CName<T,?> cName, SqlKeyword op, Object value) {
        return and(Cndx.expEX(cName, op, value));
    }
    public Cndx orEX(CName<T,?> cName, SqlKeyword op, Object value) {
        return or(Cndx.expEX(cName, op, value));
    }

    public Cndx<T> eq(CName<T,?> cName, Object value) {
        return and(Cndx.exp(cName, SqlKeyword.EQ, value));
    }
    public Cndx<T> orEq(CName<T,?> cName, Object value) {
        return or(Cndx.exp(cName, SqlKeyword.EQ, value));
    }
    public Cndx<T> ne(CName<T,?> cName, Object value) {
        return and(Cndx.exp(cName, SqlKeyword.NE, value));
    }
    public Cndx<T> orNe(CName<T,?> cName, Object value) {
        return or(Cndx.exp(cName, SqlKeyword.NE, value));
    }
    public Cndx<T> in(CName<T,?> cName, Collection<?> value) {
        return and(Cndx.exp(cName, SqlKeyword.IN, value));
    }
    public Cndx<T> orIn(CName<T,?> cName, Collection<?> value) {
        return or(Cndx.exp(cName, SqlKeyword.IN, value));
    }
    public Cndx<T> notIn(CName<T,?> cName, Collection<?> value) {
        return and(Cndx.exp(cName, SqlKeyword.NOTIN, value));
    }
    public Cndx<T> orNotIn(CName<T,?> cName, Collection<?> value) {
        return or(Cndx.exp(cName, SqlKeyword.NOTIN, value));
    }
    public Cndx<T> like(CName<T,?> cName, String value) {
        cri.where().andLike(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orLike(CName<T,?> cName, String value) {
        cri.where().orLike(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> notLike(CName<T,?> cName, String value) {
        cri.where().andNotLike(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orNotLike(CName<T,?> cName, String value) {
        cri.where().orNotLike(getColumnName(cName),value);
        return this;
    }

    public Cndx<T> ge(CName<T,?> cName, Long value) {
        cri.where().andGTE(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orGe(CName<T,?> cName, Long value) {
        cri.where().orGTE(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> gt(CName<T,?> cName, Long value) {
        cri.where().andGT(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orGt(CName<T,?> cName, Long value) {
        cri.where().orGT(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> le(CName<T,?> cName, Long value) {
        cri.where().andLTE(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orLe(CName<T,?> cName, Long value) {
        cri.where().orLTE(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> lt(CName<T,?> cName, Long value) {
        cri.where().andLT(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orLt(CName<T,?> cName, Long value) {
        cri.where().orLT(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> between(CName<T,?> cName, Object min, Object max){
        cri.where().andBetween(getColumnName(cName),min,max);
        return this;
    }
    public Cndx<T> orBetween(CName<T,?> cName, Object min, Object max){
        cri.where().orBetween(getColumnName(cName),min,max);
        return this;
    }
    public Cndx<T> likeL(CName<T,?> cName, String value){
        cri.where().andLikeL(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orlikeL(CName<T,?> cName, String value){
        cri.where().orLikeL(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> likeR(CName<T,?> cName, String value){
        cri.where().andLikeR(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> orLikeR(CName<T,?> cName, String value){
        cri.where().orLikeR(getColumnName(cName),value);
        return this;
    }
    public Cndx<T> isNull(CName<T,?> cName){
        cri.where().andIsNull(getColumnName(cName));
        return this;
    }
    public Cndx<T> orIsNull(CName<T,?> cName){
        cri.where().orIsNull(getColumnName(cName));
        return this;
    }
    public Cndx<T> isNotNull(CName<T,?> cName){
        cri.where().andNotIsNull(getColumnName(cName));
        return this;
    }
    public Cndx<T> orIsNotNull(CName<T,?> cName){
        cri.where().orNotIsNull(getColumnName(cName));
        return this;
    }

    /**
     *  =============以下全部拷贝 Cnd===================
     */
    private static final long serialVersionUID = 1L;

    /**
     * 用字符串和参数格式化出一个条件语句,注意,不会抹除特殊字符
     * @param format sql条件
     * @param args 参数
     * @return 条件对象
     */
    public static Condition format(String format, Object... args) {
        return Strings.isBlank(format) ? null : new SimpleCondition(format,
                args);
    }

    /***
     * 直接用字符串生成一个条件对象
     * @param str sql条件
     * @return 条件对象
     */
    public static Condition wrap(String str) {
        return Strings.isBlank(str) ? null : new SimpleCondition((Object) str);
    }

    /**
     * 使用CharSegment拼装一个条件对象
     * @param sql sql模板
     * @param value 参数
     * @return 条件对象
     * @see CharSegment
     */
    public static Condition wrap(String sql, Object value) {
        return Strings.isBlank(sql) ? null
                : new SimpleCondition(new CharSegment(sql).setBy(value));
    }

    /**
     * 生成一个条件表达式
     * @param name Java属性或字段名称
     * @param op   操作符,可以是 = like 等等
     * @param value 参数值.
     * @return 条件表达式
     */
    public static SqlExpression exp(String name, String op, Object value) {
        if(value!=null && value instanceof Nesting){
            return NestExps.create(name, op, (Nesting) value);
        }
        return Exps.create(name, op, value);
    }

    /**
     * 生成一个条件表达式组
     * @param name Java属性或字段名称
     * @param op   操作符,可以是 = like 等等
     * @param value 参数值.
     * @return 条件表达式组
     */
    public static SqlExpressionGroup exps(String name, String op, Object value) {
        return exps(exp(name, op, value));
    }

    /**
     * 将一个条件表达式封装为条件表达式组
     * @param exp 原本的条件表达式
     * @return 条件表达式组
     */
    public static SqlExpressionGroup exps(SqlExpression exp) {
        return new SqlExpressionGroup().and(exp);
    }

    /**
     * 生成一个新的Cnd实例
     * @param name java属性或字段名称, 推荐用Java属性
     * @param op 操作符,可以是= like等等
     * @param value 参数值. 如果操作符是between,参数值需要是new Object[]{12,39}形式
     * @return Cnd实例
     */
    public static Cndx where(String name, String op, Object value) {
        return new Cndx(Cndx.exp(name, op, value));
    }

    /**
     * 用一个条件表达式构建一个Cnd实例
     * @param e 条件表达式
     * @return Cnd实例
     */
    public static Cndx where(SqlExpression e) {
        return new Cndx(e);
    }

    /**
     * 生成一个简单条件对象
     */
    public static SimpleCriteria cri() {
        return new SimpleCriteria();
    }

    /**
     * 单纯生成一个Orderby条件
     * @return OrderBy实例
     */
    public static OrderBy orderBy() {
        return new Cndx();
    }

    /**
     * @return 一个 Cnd 的实例
     * @deprecated Since 1.b.50 不推荐使用这个函数构建 Cnd 的实例，因为看起来语意不明的样子
     */
    public static Cndx limit() {
        return new Cndx();
    }

    /**
     * @return 一个 Cnd 的实例
     */
    public static Cndx NEW() {
        return new Cndx();
    }


    /**
     * 用SimpleCriteria生成一个Cnd实例
     * @param cri SimpleCriteria实例
     * @return Cnd实例
     */
    public static Cndx byCri(SimpleCriteria cri) {
        return new Cndx().setCri(cri);
    }

    /*------------------------------------------------------------------*/

    protected SimpleCriteria cri;

    protected Cndx() {
        cri = new SimpleCriteria();
    }

    private Cndx setCri(SimpleCriteria cri) {
        this.cri = cri;
        return this;
    }

    /**
     * 获取内部的where属性
     * @return SimpleCriteria实例
     */
    public SimpleCriteria getCri() {
        return cri;
    }

    protected Cndx(SqlExpression exp) {
        this();
        cri.where().and(exp);
    }

    /**
     * 按Java属性/字段属性进行升序. <b>不进行SQL特殊字符抹除<b/>  cnd.asc("age")
     * @param name Java属性/字段属性
     */
    public OrderBy asc(String name) {
        cri.asc(name);
        return this;
    }

    /**
     * 按Java属性/字段属性进行降序. <b>不进行SQL特殊字符抹除<b/> cnd.desc("age")
     * @param name Java属性/字段属性
     */
    public OrderBy desc(String name) {
        cri.desc(name);
        return this;
    }

    /**
     * 当dir为asc时判断为升序,否则判定为降序. cnd.orderBy("age", "asc")
     * @param name Java属性/字段属性
     * @param dir asc或其他
     * @return OrderBy实例,事实上就是当前对象
     */
    public OrderBy orderBy(String name, String dir) {
        if ("asc".equalsIgnoreCase(dir)) {
            this.asc(name);
        } else {
            this.desc(name);
        }
        return this;
    }

    /**
     * Cnd.where(...).and(Cnd.exp(.........)) 或 Cnd.where(...).and(Cnd.exps(.........))
     * @param exp 条件表达式
     * @return 当前对象,用于链式调用
     */
    public Cndx and(SqlExpression exp) {
        cri.where().and(exp);
        return this;
    }

    /**
     * Cnd.where(...).and("age", "<", 40)
     * @param name Java属性或字段名称,推荐用Java属性,如果有的话
     * @param op 操作符,可以是 = like等
     * @param value 参数值, 如果是between的话需要传入new Object[]{19,28}
     * @return 当前对象,用于链式调用
     */
    public Cndx and(String name, String op, Object value) {
        return and(Cndx.exp(name, op, value));
    }

    /**
     * Cnd.where(...).or(Cnd.exp(.........)) 或 Cnd.where(...).or(Cnd.exps(.........))
     * @param exp 条件表达式
     * @return 当前对象,用于链式调用
     */
    public Cndx or(SqlExpression exp) {
        cri.where().or(exp);
        return this;
    }

    /**
     * Cnd.where(...).or("age", "<", 40)
     * @param name Java属性或字段名称,推荐用Java属性,如果有的话
     * @param op 操作符,可以是 = like等
     * @param value 参数值, 如果是between的话需要传入new Object[]{19,28}
     * @return 当前对象,用于链式调用
     */
    public Cndx or(String name, String op, Object value) {
        return or(Cndx.exp(name, op, value));
    }

    /**
     * and一个条件表达式并且取非
     * @param exp 条件表达式
     * @return 当前对象,用于链式调用
     */
    public Cndx andNot(SqlExpression exp) {
        cri.where().and(exp.setNot(true));
        return this;
    }

    /**
     * and一个条件,并且取非
     * @param name Java属性或字段名称,推荐用Java属性,如果有的话
     * @param op 操作符,可以是 = like等
     * @param value 参数值, 如果是between的话需要传入new Object[]{19,28}
     * @return 当前对象,用于链式调用
     */
    public Cndx andNot(String name, String op, Object value) {
        return andNot(Cndx.exp(name, op, value));
    }

    /**
     * @see Cndx#andNot(SqlExpression)
     */
    public Cndx orNot(SqlExpression exp) {
        cri.where().or(exp.setNot(true));
        return this;
    }
    /**
     * @see Cndx#andNot(String, String, Object)
     */
    public Cndx orNot(String name, String op, Object value) {
        return orNot(Cndx.exp(name, op, value));
    }

    /**
     * 获取分页对象,默认是null
     */
    public Pager getPager() {
        return cri.getPager();
    }

    /**
     * 根据实体Entity将本对象转化为sql语句, 条件表达式中的name属性将转化为数据库字段名称
     */
    public String toSql(Entity<?> en) {
        return cri.toSql(en);
    }

    /**
     * 判断两个Cnd是否相等
     */
    public boolean equals(Object obj) {
        return cri.equals(obj);
    }

    /**
     * 直接转为SQL语句, 如果setPojo未曾调用, 条件表达式中的name属性未映射为数据库字段
     */
    public String toString() {
        return cri.toString();
    }

    /**
     * 关联的Pojo,可以用于toString时的name属性映射
     */
    public void setPojo(Pojo pojo) {
        cri.setPojo(pojo);
    }

    /**
     * 获取已设置的Pojo, 默认为null
     */
    public Pojo getPojo() {
        return cri.getPojo();
    }

    public void joinSql(Entity<?> en, StringBuilder sb) {
        cri.joinSql(en, sb);
    }

    public int joinAdaptor(Entity<?> en, ValueAdaptor[] adaptors, int off) {
        return cri.joinAdaptor(en, adaptors, off);
    }

    public int joinParams(Entity<?> en, Object obj, Object[] params, int off) {
        return cri.joinParams(en, obj, params, off);
    }

    public int paramCount(Entity<?> en) {
        return cri.paramCount(en);
    }

    /**
     * 获取Cnd中的where部分,注意,对SqlExpressionGroup的修改也会反映到Cnd中,因为是同一个对象
     */
    public SqlExpressionGroup where() {
        return cri.where();
    }

    /**
     * 分组
     * @param names java属性或数据库字段名称
     */
    public GroupBy groupBy(String... names) {
        cri.groupBy(names);
        return this;
    }

    /**
     * 分组中的having条件
     * @param cnd 条件语句
     */
    public GroupBy having(Condition cnd) {
        cri.having(cnd);
        return this;
    }

    /**
     * 单独获取排序条件,建议使用asc或desc,而非直接取出排序条件. 取出的对象仅包含分组条件, 不包含where等部分
     */
    public OrderBy getOrderBy() {
        return cri.getOrderBy();
    }

    /**
     * 分页
     * @param pageNumber 页数, 若小于1则代表全部记录
     * @param pageSize 每页数量
     * @return 当前对象,用于链式调用
     */
    public Cndx limit(int pageNumber, int pageSize) {
        cri.setPager(pageNumber, pageSize);
        return this;
    }

    /**
     * 设置每页大小,并设置页数为1
     * @param pageSize 每页大小
     * @return 当前对象,用于链式调用
     */
    @Deprecated
    public Cndx limit(int pageSize) {
        cri.setPager(1, pageSize);
        return this;
    }

    /**
     * 直接设置分页对象, 可以new Pager或dao.createPager得到
     * @param pager 分页对象
     * @return 当前对象,用于链式调用
     */
    public Cndx limit(Pager pager) {
        cri.setPager(pager);
        return this;
    }

    protected static FieldMatcher dftFromFieldMatcher = new FieldMatcher().setIgnoreNull(true).setIgnoreZero(true);

    /**
     * 用默认规则(忽略零值和空值)生成Cnd实例
     * @param dao Dao实例,不能为null
     * @param obj 对象, 若为null,则返回值为null, 不可以是Class/字符串/数值/布尔类型
     * @return Cnd实例
     */
    public static Cndx from(Dao dao, Object obj) {
        return from(dao, obj, dftFromFieldMatcher);
    }

    /**
     * 根据一个对象生成Cnd条件, FieldMatcher详细控制.<p/>
     * <code>assertEquals(" WHERE name='wendal' AND age=0", Cnd.from(dao, pet, FieldMatcher.make("age|name", null, true).setIgnoreDate(true)).toString());</code>
     * @param dao Dao实例
     * @param obj 基对象,不可以是Class,字符串,数值和Boolean
     * @param matcher 过滤字段属性, 可配置哪些字段可用/不可用/是否忽略空值/是否忽略0值/是否忽略java.util.Date类及其子类的对象/是否忽略@Id所标注的主键属性/是否忽略 \@Name 所标注的主键属性/是否忽略 \@Pk 所引用的复合主键
     * @return Cnd条件
     */
    public static Cndx from(Dao dao, Object obj, FieldMatcher matcher) {
        final SqlExpressionGroup exps = new SqlExpressionGroup();
        boolean re = Daos.filterFields(obj, matcher, dao, new Callback2<MappingField, Object>() {
            public void invoke(MappingField mf, Object val) {
                exps.and(mf.getName(), "=", val);
            }
        });
        if (re)
            return Cndx.where(exps);
        return null;
    }

    /**
     * 若value为null/空白字符串/空集合/空数组,则本条件不添加.
     * @see Cndx#and(String, String, Object)
     */
    public Cndx andEX(String name, String op, Object value) {
        return and(Cndx.expEX(name, op, value));
    }

    /**
     * 若value为null/空白字符串/空集合/空数组,则本条件不添加.
     * @see Cndx#or(String, String, Object)
     */
    public Cndx orEX(String name, String op, Object value) {
        return or(Cndx.expEX(name, op, value));
    }

    public static SqlExpression expEX(String name, String op, Object value) {
        if (_ex(value))
            return null;
        return Cndx.exp(name, op, value);
    }

    @SuppressWarnings("rawtypes")
    public static boolean _ex(Object value) {
        return value == null
                || (value instanceof CharSequence && Strings.isBlank((CharSequence)value))
                || (value instanceof Collection && ((Collection)value).isEmpty())
                || (value.getClass().isArray() && Array.getLength(value) == 0);
    }

    public GroupBy getGroupBy() {
        return cri.getGroupBy();
    }

    /**
     * 构造一个可嵌套条件，需要dao支持才能映射类与表和属性与列
     */
    public static Nesting nst(Dao dao){
        return new SimpleNesting(dao);
    }

    /**
     * 克隆当前Cnd实例
     * @return 一模一样的兄弟
     */
    public Cndx clone() {
        return Lang.fromBytes(Lang.toBytes(this), Cndx.class);
    }
    
    /**
     * 仅拷贝where条件, 不拷贝排序/分组/分页
     */
    public Cndx cloneWhere() {
        return Cndx.where(this.cri.where().clone());
    }
}
