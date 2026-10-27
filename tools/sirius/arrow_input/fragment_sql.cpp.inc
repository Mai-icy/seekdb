void Fragment::declare_input_cardinality(std::uint64_t stream_id, std::uint64_t rows)
{
  impl_->require_not_built("declare_input_cardinality");
  impl_->input_cardinalities[stream_id] = rows;
}

// Experimental SQL planning over declared streams; execution remains Sirius-only.
void Fragment::build_sql(const std::string &sql, const std::vector<std::string> &tables)
{
  impl_->require_not_built("build_sql");
  if (!impl_->outputs.empty()) throw std::invalid_argument("build_sql requires a result fragment");
  if (tables.size() != impl_->inputs.size()) {
    throw std::invalid_argument("Input table-name count does not match declared streams");
  }
  std::set<std::string> seen;
  for (const auto &name : tables) {
    if (name.empty() || name.size() > 64 || !(name[0] >= 'a' && name[0] <= 'z')
        || name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") != std::string::npos
        || !seen.insert(name).second) {
      throw std::invalid_argument("Input names must be unique lowercase SQL identifiers");
    }
  }
  duckdb::Parser parser;
  parser.ParseQuery(sql);
  if (parser.statements.size() != 1
      || parser.statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
    throw std::invalid_argument("Exactly one SELECT statement is required");
  }
  std::unique_ptr<sirius::exec::streaming_fragment> fragment;
  in_transaction(impl_->ctx.conn_mutex, *impl_->ctx.conn, [&] {
    sirius::exec::fragment_spec spec;
    spec.inputs = impl_->resolve_inputs();
    spec.plan_source = [&](duckdb::ClientContext &client) -> sirius::exec::bound_plan {
      // A stream is consumptive. Materialize each declared table once on GPU;
      // self joins and correlated subqueries then read the GPU CTE repeatedly.
      std::string ctes = "WITH ";
      size_t position = 0;
      for (const auto &[id, input] : impl_->inputs) {
        if (position) ctes += ",";
        ctes += "\"" + tables[position++] + "\" AS MATERIALIZED (SELECT * FROM sirius_stream_source("
             + std::to_string(id) + "))";
      }
      duckdb::Parser cte_parser;
      cte_parser.ParseQuery(ctes + " SELECT 1");
      auto &target = parser.statements[0]->Cast<duckdb::SelectStatement>().node->cte_map.map;
      auto &source = cte_parser.statements[0]->Cast<duckdb::SelectStatement>().node->cte_map.map;
      // Base tables must precede the user's CTEs: SQL only binds earlier CTEs.
      for (auto &entry : target) {
        if (source.find(entry.first) != source.end()) throw std::invalid_argument("Input table shadows an SQL CTE");
        source[entry.first] = std::move(entry.second);
      }
      target = std::move(source);
      duckdb::Planner planner(client);
      planner.CreatePlan(std::move(parser.statements[0]));
      auto prepared = duckdb::make_shared_ptr<duckdb::PreparedStatementData>(
          duckdb::StatementType::SELECT_STATEMENT);
      prepared->names = planner.names;
      prepared->types = planner.types;
      prepared->value_map = std::move(planner.value_map);
      auto plan = std::move(planner.plan);
      std::function<void(duckdb::LogicalOperator &)> cardinalities = [&](auto &op) {
        if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
          auto &get = op.template Cast<duckdb::LogicalGet>();
          if (get.function.name == sirius::exec::kStreamSourceFunctionName) {
            const auto &bind = get.bind_data->template Cast<sirius::exec::stream_source_bind_data>();
            const auto found = impl_->input_cardinalities.find(bind.stream_id);
            if (found != impl_->input_cardinalities.end()) get.SetEstimatedCardinality(found->second);
          } else {
            throw std::invalid_argument("Only declared Arrow table scans are allowed");
          }
        }
        for (auto &child : op.children) cardinalities(*child);
      };
      cardinalities(*plan);
      duckdb::Optimizer optimizer(*planner.binder, client);
      plan = optimizer.Optimize(std::move(plan));
      plan->ResolveOperatorTypes();
      duckdb::ColumnBindingResolver verifier(true), resolver;
      verifier.VisitOperator(*plan);
      resolver.VisitOperator(*plan);
      return {std::move(plan), std::move(prepared)};
    };
    fragment = std::make_unique<sirius::exec::streaming_fragment>(
        *impl_->ctx.conn->context, std::move(spec));
    fragment->build();
  });
  impl_->fragment = std::move(fragment);
}
