package org.eclipse.persistence.platform.database;

import org.eclipse.persistence.internal.databaseaccess.DatabaseCall;
import org.eclipse.persistence.internal.expressions.*;

/**
 * @author rsinyakov
 * @since 17.05.2016
 */
public class SQLServer2012Platform extends SQLServerPlatform {

    private static final String FETCH = " FETCH ";
    private static final String OFFSET = " OFFSET ";
    private static final String ONLY = " ONLY ";
    private static final String ROWS = " ROWS ";
    private static final String NEXT = " NEXT ";

    @Override
    public void printSQLSelectStatement(DatabaseCall call, ExpressionSQLPrinter printer, SQLSelectStatement statement) {
        int max = 0;
        if (statement.getQuery() != null) {
            max = statement.getQuery().getMaxRows();
        }
        if (max <= 0 || !(this.shouldUseRownumFiltering()) || !statement.hasOrderByExpressions()) {
            super.printSQLSelectStatement(call, printer, statement);
            return;
        }
        statement.setUseUniqueFieldAliases(true);//?
        call.setFields(statement.printSQL(printer));
        printer.printString(OFFSET);
        printer.printParameter(DatabaseCall.FIRSTRESULT_FIELD);
        printer.printString(ROWS);
        printer.printString(FETCH);
        printer.printString(NEXT);
        printer.printParameter(DatabaseCall.MAXROW_FIELD);
        printer.printString(ROWS);
        printer.printString(ONLY);
        call.setIgnoreFirstRowSetting(true);
        call.setIgnoreMaxResultsSetting(true);
    }

    @Override
    public int computeMaxRowsForSQL(int firstResultIndex, int maxResults) {
        return maxResults - ((firstResultIndex >= 0) ? firstResultIndex : 0);
    }
}
