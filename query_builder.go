package redifu

import (
	"fmt"
	"strings"
)

const (
	Between          = "between"
	LowerThan        = "lt"
	GreaterThan      = "gt"
	LowerThanEqual   = "lte"
	GreaterThanEqual = "gte"
	Equal            = "eq"
	NotEqual         = "neq"
	And              = "AND"
	Or               = "OR"
)

type Builder struct {
	table          string
	alias          string
	selectCols     string
	joins          []string
	orderCol       string
	orderDir       string
	conditions     []condition
	cursorOperator string   // Store cursor operator for WithCursor
	groupByCols    []string // Add this field
}

type condition struct {
	Column   string
	Operator string // "between", "lt", "gt", "lte", "gte", "eq", "neq"
	Joiner   string // "AND" or "OR" - how to join with the next condition
}

func NewQuery(table string, alias ...string) *Builder {
	b := &Builder{
		table:       table,
		orderDir:    Descending,
		joins:       []string{},
		conditions:  []condition{},
		groupByCols: []string{}, // Initialize this
	}

	if len(alias) > 0 {
		b.alias = alias[0]
	}

	return b
}

func (b *Builder) Select(cols string) *Builder {
	b.selectCols = cols
	return b
}

func (b *Builder) LeftJoin(table string, aliasOrCondition string, condition ...string) *Builder {
	var join string
	if len(condition) > 0 {
		join = fmt.Sprintf("LEFT JOIN %s %s ON %s", table, aliasOrCondition, condition[0])
	} else {
		join = fmt.Sprintf("LEFT JOIN %s ON %s", table, aliasOrCondition)
	}
	b.joins = append(b.joins, join)
	return b
}

func (b *Builder) InnerJoin(table string, aliasOrCondition string, condition ...string) *Builder {
	var join string
	if len(condition) > 0 {
		join = fmt.Sprintf("INNER JOIN %s %s ON %s", table, aliasOrCondition, condition[0])
	} else {
		join = fmt.Sprintf("INNER JOIN %s ON %s", table, aliasOrCondition)
	}
	b.joins = append(b.joins, join)
	return b
}

func (b *Builder) OrderBy(col, direction string) *Builder {
	b.orderCol = col
	b.orderDir = direction
	return b
}

// Where adds a condition with optional joiner (AND/OR) for the next condition
// joiner defaults to "AND" if not specified
func (b *Builder) Where(column, operator string, joiner ...string) *Builder {
	join := "AND" // default
	if len(joiner) > 0 {
		join = strings.ToUpper(joiner[0])
	}

	b.conditions = append(b.conditions, condition{
		Column:   column,
		Operator: operator,
		Joiner:   join,
	})
	return b
}

// Add this method
func (b *Builder) GroupBy(cols ...string) *Builder {
	b.groupByCols = append(b.groupByCols, cols...)
	return b
}

// Cursor sets the operator for cursor-based pagination
// This allows users to specify how the cursor should compare (lt, lte, gt, gte, eq)
func (b *Builder) Cursor(operator string) *Builder {
	b.cursorOperator = operator
	return b
}

func (b *Builder) tableRef() string {
	if b.alias != "" {
		return fmt.Sprintf("%s %s", b.table, b.alias)
	}
	return b.table
}

func (b *Builder) Row(idCol string) string {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(b.selectCols)
	query.WriteString("\nFROM ")
	query.WriteString(b.tableRef())

	for _, join := range b.joins {
		query.WriteString("\n")
		query.WriteString(join)
	}

	var whereClause string
	if b.alias != "" {
		whereClause = fmt.Sprintf("\nWHERE %s.%s = $1", b.alias, idCol)
	} else {
		whereClause = fmt.Sprintf("\nWHERE %s = $1", idCol)
	}

	query.WriteString(whereClause)

	// Add GROUP BY - INSERT THIS BLOCK
	if len(b.groupByCols) > 0 {
		query.WriteString("\nGROUP BY ")
		query.WriteString(strings.Join(b.groupByCols, ", "))
	}

	return query.String()
}

func (b *Builder) Base() string {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(b.selectCols)
	query.WriteString("\nFROM ")
	query.WriteString(b.tableRef())

	for _, join := range b.joins {
		query.WriteString("\n")
		query.WriteString(join)
	}

	// Add user conditions if any
	if len(b.conditions) > 0 {
		query.WriteString("\nWHERE ")
		b.buildConditions(&query, 1)
	}

	// Add GROUP BY - INSERT THIS BLOCK
	if len(b.groupByCols) > 0 {
		query.WriteString("\nGROUP BY ")
		query.WriteString(strings.Join(b.groupByCols, ", "))
	}

	// Add ORDER BY
	if b.orderCol != "" {
		query.WriteString("\nORDER BY ")
		query.WriteString(b.orderCol)
		query.WriteString(" ")
		query.WriteString(b.orderDir)
	}

	return query.String()
}

func (b *Builder) WithCursor() string {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(b.selectCols)
	query.WriteString("\nFROM ")
	query.WriteString(b.tableRef())

	for _, join := range b.joins {
		query.WriteString("\n")
		query.WriteString(join)
	}

	query.WriteString("\nWHERE ")

	paramIdx := 1

	// Add user conditions first
	if len(b.conditions) > 0 {
		paramIdx = b.buildConditions(&query, paramIdx)
		query.WriteString(" AND ")
	}

	// Determine cursor operator
	var cursorOp string
	if b.cursorOperator != "" {
		// User specified operator via Cursor()
		switch b.cursorOperator {
		case LowerThan, "<":
			cursorOp = "<"
		case GreaterThan, ">":
			cursorOp = ">"
		case LowerThanEqual, "<=":
			cursorOp = "<="
		case GreaterThanEqual, ">=":
			cursorOp = ">="
		case Equal, "=":
			cursorOp = "="
		case NotEqual, "!=", "<>":
			cursorOp = "!="
		default:
			// Default based on order direction
			if b.orderDir == Ascending {
				cursorOp = ">"
			} else {
				cursorOp = "<"
			}
		}
	} else {
		// Default based on order direction
		if b.orderDir == Ascending {
			cursorOp = ">"
		} else {
			cursorOp = "<"
		}
	}

	// Add cursor condition
	query.WriteString(b.orderCol)
	query.WriteString(" ")
	query.WriteString(cursorOp)
	query.WriteString(" $")
	query.WriteString(fmt.Sprintf("%d", paramIdx))

	// Add GROUP BY - INSERT THIS BLOCK
	if len(b.groupByCols) > 0 {
		query.WriteString("\nGROUP BY ")
		query.WriteString(strings.Join(b.groupByCols, ", "))
	}

	// Add ORDER BY
	query.WriteString("\nORDER BY ")
	query.WriteString(b.orderCol)
	query.WriteString(" ")
	query.WriteString(b.orderDir)

	return query.String()
}

// buildConditions builds WHERE conditions and returns the next parameter index
func (b *Builder) buildConditions(query *strings.Builder, startParamIdx int) int {
	paramIdx := startParamIdx

	for i, cond := range b.conditions {
		if i > 0 {
			// Use the joiner from the previous condition
			query.WriteString(" ")
			query.WriteString(b.conditions[i-1].Joiner)
			query.WriteString(" ")
		}

		switch cond.Operator {
		case Between:
			query.WriteString(cond.Column)
			query.WriteString(" BETWEEN $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			query.WriteString(" AND $")
			query.WriteString(fmt.Sprintf("%d", paramIdx+1))
			paramIdx += 2
		case GreaterThan, ">":
			query.WriteString(cond.Column)
			query.WriteString(" > $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			paramIdx++
		case LowerThan, "<":
			query.WriteString(cond.Column)
			query.WriteString(" < $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			paramIdx++
		case GreaterThanEqual, ">=":
			query.WriteString(cond.Column)
			query.WriteString(" >= $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			paramIdx++
		case LowerThanEqual, "<=":
			query.WriteString(cond.Column)
			query.WriteString(" <= $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			paramIdx++
		case Equal, "=":
			query.WriteString(cond.Column)
			query.WriteString(" = $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			paramIdx++
		case NotEqual, "!=", "<>":
			query.WriteString(cond.Column)
			query.WriteString(" != $")
			query.WriteString(fmt.Sprintf("%d", paramIdx))
			paramIdx++
		}
	}

	return paramIdx
}
