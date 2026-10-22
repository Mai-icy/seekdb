#!/usr/bin/env python3
"""Build a small Substrait Parquet scan/filter/aggregate plan for Sirius."""
import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from substrait import algebra_pb2 as alg, plan_pb2


def field(index):
    expr = alg.Expression()
    expr.selection.direct_reference.struct_field.field = index
    expr.selection.root_reference.SetInParent()
    return expr


def make_plan(parquet, groups=(), sums=(), count=False, minimum=None):
    parquet = Path(parquet).resolve(strict=True)
    schema = pq.read_schema(parquet)
    names = schema.names
    if len(set(names)) != len(names):
        raise ValueError("Duplicate Parquet column names are unsupported")
    plan = plan_pb2.Plan()
    plan.version.minor_number = 85
    plan.version.producer = "seekdb-sirius-prototype"

    def function(name):
        ext = plan.extensions.add().extension_function
        ext.function_anchor = len(plan.extensions)
        ext.name = name
        return ext.function_anchor

    rel = alg.Rel()
    item = rel.read.local_files.items.add()
    item.uri_file = str(parquet)
    item.parquet.SetInParent()
    if minimum:
        name, value = minimum.split("=", 1)
        filtered = alg.Rel()
        filtered.filter.input.CopyFrom(rel)
        fn = filtered.filter.condition.scalar_function
        fn.function_reference = function("gte")
        fn.arguments.add().value.CopyFrom(field(names.index(name)))
        literal = fn.arguments.add().value.literal
        if pa.types.is_integer(schema.field(name).type):
            literal.i64 = int(value)
        elif pa.types.is_floating(schema.field(name).type):
            literal.fp64 = float(value)
        else:
            raise ValueError("--minimum requires a numeric column")
        fn.output_type.bool.nullability = 1
        rel = filtered
    output_names = names
    if groups or sums or count:
        aggregate = alg.Rel()
        agg = aggregate.aggregate
        agg.input.CopyFrom(rel)
        grouping = agg.groupings.add()
        for name in groups:
            grouping.expression_references.append(len(agg.grouping_expressions))
            agg.grouping_expressions.add().CopyFrom(field(names.index(name)))
        for name in sums:
            if not (pa.types.is_integer(schema.field(name).type)
                    or pa.types.is_floating(schema.field(name).type)):
                raise ValueError("SUM helper supports integer and floating point columns")
            measure = agg.measures.add().measure
            measure.function_reference = function("sum")
            measure.arguments.add().value.CopyFrom(field(names.index(name)))
            measure.phase = alg.AGGREGATION_PHASE_INITIAL_TO_RESULT
            measure.invocation = alg.AggregateFunction.AGGREGATION_INVOCATION_ALL
            if pa.types.is_floating(schema.field(name).type):
                measure.output_type.fp64.nullability = 1
            else:
                measure.output_type.decimal.precision = 38
                measure.output_type.decimal.scale = 0
                measure.output_type.decimal.nullability = 1
        if count:
            measure = agg.measures.add().measure
            measure.function_reference = function("count")
            measure.phase = alg.AGGREGATION_PHASE_INITIAL_TO_RESULT
            measure.invocation = alg.AggregateFunction.AGGREGATION_INVOCATION_ALL
            measure.output_type.i64.nullability = 2
        output_names = list(groups) + ["sum_" + name for name in sums] + (["count"] if count else [])
        rel = aggregate
    root = plan.relations.add().root
    root.input.CopyFrom(rel)
    root.names.extend(output_names)
    return plan.SerializeToString()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parquet")
    parser.add_argument("output")
    parser.add_argument("--group-by", action="append", default=[])
    parser.add_argument("--sum", action="append", default=[], dest="sums")
    parser.add_argument("--count", action="store_true")
    parser.add_argument("--minimum", help="Numeric filter COLUMN=VALUE (inclusive)")
    args = parser.parse_args()
    Path(args.output).write_bytes(make_plan(args.parquet, args.group_by, args.sums, args.count, args.minimum))
