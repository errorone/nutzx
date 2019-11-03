package org.nutz.dao;

public enum SqlKeyword{
        AND("AND"),
        OR("OR"),
        IN("IN"),
        NOTIN("NOT IN"),
        LIKE("LIKE"),
        NOTLIKE("NOT LIKE"),
        EQ("="),
        NE("<>"),
        GT(">"),
        GE(">="),
        LT("<"),
        LE("<="),
        IS_NULL("IS NULL"),
        IS_NOT_NULL("IS NOT NULL"),
        GROUP_BY("GROUP BY"),
        HAVING("HAVING"),
        ORDER_BY("ORDER BY"),
        EXISTS("EXISTS"),
        BETWEEN("BETWEEN"),
        ASC("ASC"),
        DESC("DESC");

        private final String keyword;

        SqlKeyword(final String keyword) {
            this.keyword = keyword;
        }

        @Override
        public String toString() {
            return this.keyword;
        }
    }