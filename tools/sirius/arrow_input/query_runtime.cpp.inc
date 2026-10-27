  size_t execute_query(const char *sql, size_t sql_size, SeekdbSiriusTableV1 *tables,
                       size_t count, SeekdbSiriusResultV1 consume, void *user) {
    return submit([&](auto &context) -> size_t {
      std::vector<ArrowSchema> schemas(count);
      struct InputsGuard {
        SeekdbSiriusTableV1 *tables;
        std::vector<ArrowSchema> &schemas;
        ~InputsGuard() {
          for (size_t i = 0; i < schemas.size(); ++i) {
            if (schemas[i].release) schemas[i].release(&schemas[i]);
            auto &stream = *tables[i].stream;
            if (stream.release) stream.release(&stream);
          }
        }
      } guard{tables, schemas};
      auto fragment = sirius::ffi::make_fragment(context);
      std::vector<std::string> names;
      for (size_t i = 0; i < count; ++i) {
        auto &input = *tables[i].stream;
        auto &schema = schemas[i];
        if (!input.release || !input.get_schema || !input.get_next
            || input.get_schema(&input, &schema) || !schema.release || !schema.format
            || std::string(schema.format) != "+s" || schema.dictionary
            || schema.n_children < 1 || schema.n_children > 64 || !schema.children) {
          throw std::invalid_argument("Invalid query input stream/schema");
        }
        for (int64_t c = 0; c < schema.n_children; ++c) {
          auto *column = schema.children[c];
          if (!column || !column->name) throw std::invalid_argument("Unnamed input column");
          fragment->declare_input_column(i, column->name, seekdb_sirius::arrow_type(*column).sql);
        }
        fragment->declare_input_cardinality(i, tables[i].estimated_rows);
        names.emplace_back(tables[i].name);
      }
      fragment->build_sql(std::string(sql, sql_size), names);
      size_t bytes = 0;
      constexpr size_t limit = 512 * 1024 * 1024;
      for (size_t i = 0; i < count; ++i) {
        auto &input = *tables[i].stream;
        bool received = false;
        for (;;) {
          ArrowArray batch{};
          struct BatchGuard { ArrowArray &a; ~BatchGuard() { if (a.release) a.release(&a); } } bg{batch};
          if (input.get_next(&input, &batch)) {
            const char *error = input.get_last_error ? input.get_last_error(&input) : nullptr;
            throw std::runtime_error(error ? error : "Query input producer failed");
          }
          if (!batch.release) break;
          const auto size = seekdb_sirius::arrow_batch_bytes(schemas[i], batch);
          if (size > limit - bytes) throw std::invalid_argument("Query input exceeds 512 MiB prototype limit");
          bytes += size;
          fragment->push_arrow(i, reinterpret_cast<uintptr_t>(&schemas[i]), reinterpret_cast<uintptr_t>(&batch));
          received = true;
        }
        if (!received) {
          // Typed empty batches preserve SQL global-aggregate semantics.
          const void *buffers[3] = {nullptr, nullptr, nullptr};
          std::vector<ArrowArray> children(schemas[i].n_children);
          std::vector<ArrowArray *> pointers;
          for (size_t c = 0; c < children.size(); ++c) {
            children[c].n_buffers = seekdb_sirius::arrow_type(*schemas[i].children[c]).width == -1 ? 3 : 2;
            children[c].buffers = buffers;
            pointers.push_back(&children[c]);
          }
          ArrowArray empty{};
          empty.n_buffers = 1;
          empty.buffers = buffers;
          empty.n_children = children.size();
          empty.children = pointers.data();
          fragment->push_arrow(i, reinterpret_cast<uintptr_t>(&schemas[i]), reinterpret_cast<uintptr_t>(&empty));
        }
        fragment->close_input(i, 0);
      }
      fragment->run();
      ArrowArrayStream result{};
      struct ResultGuard { ArrowArrayStream &s; ~ResultGuard() { if (s.release) s.release(&s); } } rg{result};
      fragment->result_to_arrow(reinterpret_cast<uintptr_t>(&result));
      if (consume(&result, user)) throw std::runtime_error("Query result consumer failed");
      return 0;
    });
  }
