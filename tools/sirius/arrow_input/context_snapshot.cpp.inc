// Immutable seekdb-exported Parquet snapshots use Sirius's native pinned scan
// source, whose bounded scan tasks avoid preloading a consumptive Arrow fragment.
namespace {
std::string snapshot_literal(const std::string &value)
{
  std::string result = "'";
  for (const char c : value) { result += c; if (c == '\'') result += c; }
  return result + "'";
}
}

void Context::pin_parquet(const std::string &name, const std::string &path)
{
  if (name.empty() || name.size() > 64 || !(name[0] >= 'a' && name[0] <= 'z')
      || name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") != std::string::npos
      || path.empty() || path[0] != '/') {
    throw std::invalid_argument("Require a lowercase table name and absolute immutable Parquet path");
  }
  // PinTableFunction owns its execution window. Do not wrap it in a fragment window.
  std::lock_guard<std::mutex> lock(impl_->conn_mutex);
  auto result = impl_->conn->Query("CREATE VIEW main.\"" + name + "\" AS SELECT * FROM read_parquet("
                                  + snapshot_literal(path) + ")");
  if (result->HasError()) result->ThrowError();
  try {
    result = impl_->conn->Query("CALL pin_table(" + snapshot_literal(path)
        + ", tier => 'host', name => " + snapshot_literal(name) + ", compression => true)");
    if (result->HasError()) result->ThrowError();
  } catch (...) {
    // A failed pin must not leave a usable, silently unpinned table behind.
    impl_->conn->Query("DROP VIEW main.\"" + name + "\"");
    throw;
  }
}

void Context::execute_sql(const std::string &sql, std::uintptr_t out_stream_addr)
{
  duckdb::Parser parser;
  parser.ParseQuery(sql);
  if (parser.statements.size() != 1
      || parser.statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
    throw std::invalid_argument("Exactly one SELECT statement is required");
  }
  duckdb::unique_ptr<duckdb::QueryResult> result;
  in_transaction(impl_->conn_mutex, *impl_->conn, [&] {
    sirius::exec::fragment_spec spec;
    spec.plan_source = [&](duckdb::ClientContext &client) -> sirius::exec::bound_plan {
      duckdb::Planner planner(client);
      planner.CreatePlan(std::move(parser.statements[0]));
      auto prepared = duckdb::make_shared_ptr<duckdb::PreparedStatementData>(
          duckdb::StatementType::SELECT_STATEMENT);
      prepared->names = planner.names;
      prepared->types = planner.types;
      prepared->value_map = std::move(planner.value_map);
      auto plan = std::move(planner.plan);
      duckdb::Optimizer optimizer(*planner.binder, client);
      plan = optimizer.Optimize(std::move(plan));
      plan->ResolveOperatorTypes();
      duckdb::ColumnBindingResolver verifier(true), resolver;
      verifier.VisitOperator(*plan);
      resolver.VisitOperator(*plan);
      return {std::move(plan), std::move(prepared)};
    };
    sirius::exec::streaming_fragment fragment(*impl_->conn->context, std::move(spec));
    fragment.build();
    fragment.run();
    result = fragment.take_result();
  });
  auto *wrapper = new duckdb::ResultArrowArrayStreamWrapper(std::move(result), kArrowBatchSize);
  *reinterpret_cast<ArrowArrayStream *>(out_stream_addr) = wrapper->stream;
}
