import operator

OP_EQ = "eq"
OP_NEQ = "neq"
OP_CONTAINS = "contains"
OP_LT = "lt"
OP_GT = "gt"
OP_LTE = "lte"
OP_GTE = "gte"
OP_BETWEEN = "between"
OP_IN = "in"

NUMERIC_OPERATORS = {OP_LT, OP_GT, OP_LTE, OP_GTE}

OPERATORS = {
    OP_EQ: operator.eq,
    OP_NEQ: operator.ne,
    OP_CONTAINS: lambda val, ref: ref in str(val),
    OP_LT: operator.lt,
    OP_GT: operator.gt,
    OP_LTE: operator.le,
    OP_GTE: operator.ge,
    OP_BETWEEN: lambda val, ref: ref[0] <= str(val)[:min(len(ref[0]), len(ref[1]))] <= ref[1],
    OP_IN: lambda val, ref_set: str(val) in ref_set
}
