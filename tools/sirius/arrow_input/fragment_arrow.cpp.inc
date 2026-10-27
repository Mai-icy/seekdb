// Host Arrow input for the pinned Sirius prototype. Borrowed arrays remain
// caller-owned; the synchronized GPU copy is complete before this returns.
void Fragment::push_arrow(std::uint64_t stream_id, std::uintptr_t schema_addr,
                          std::uintptr_t array_addr)
{
  impl_->require_built("push_arrow()");
  auto *schema = reinterpret_cast<const ArrowSchema *>(schema_addr);
  auto *array = reinterpret_cast<const ArrowArray *>(array_addr);
  const auto it = impl_->inputs.find(stream_id);
  if (!schema || !array || !schema->format || std::string(schema->format) != "+s"
      || schema->dictionary || it == impl_->inputs.end()
      || schema->n_children <= 0 || schema->n_children > 64
      || schema->n_children != static_cast<int64_t>(it->second.names.size())
      || array->n_children != schema->n_children || !schema->children || !array->children
      || array->length < 0 || array->length > 8 * 1024 * 1024 || array->offset != 0
      || array->null_count != 0) {
    throw std::invalid_argument("push_arrow: invalid flat record batch/schema");
  }
  const auto decoded_bytes = (static_cast<std::uint64_t>(array->length) * 8
                              + (array->length + 7) / 8) * schema->n_children;
  if (decoded_bytes > 128 * 1024 * 1024) {
    throw std::invalid_argument("push_arrow: input batch exceeds 128 MiB");
  }
  for (int64_t i = 0; i < schema->n_children; ++i) {
    const auto *column = schema->children[i];
    const auto *values = array->children[i];
    const auto &type = it->second.type_names[i];
    const char *format = type == "BIGINT" ? "l" : type == "DOUBLE" ? "g" : "";
    if (!column || !values || !column->format || !*format
        || std::string(column->format) != format || !column->name
        || column->name != it->second.names[i] || column->dictionary || column->n_children
        || values->dictionary || values->n_children || values->offset < 0
        || values->length != array->length || values->n_buffers != 2 || !values->buffers
        || (array->length && !values->buffers[1])) {
      throw std::invalid_argument("push_arrow: input columns must match declared BIGINT/DOUBLE schema");
    }
  }
  auto &manager = impl_->ctx.context->get_memory_manager();
  const auto spaces = manager.get_memory_spaces_for_tier(cucascade::memory::Tier::GPU);
  if (spaces.empty()) { throw std::runtime_error("push_arrow: no configured GPU"); }
  const auto *target = spaces[impl_->arrow_batch_index % spaces.size()];
  auto *space = manager.get_memory_space(cucascade::memory::Tier::GPU, target->get_device_id());
  if (!space) { throw std::runtime_error("push_arrow: configured GPU is unavailable"); }
  // Respect CUDA_VISIBLE_DEVICES and the actual configured device IDs. The
  // batch carries its memory-space locality into Sirius's multi-GPU scheduler.
  rmm::cuda_set_device_raii selected{rmm::cuda_device_id{space->get_device_id()}};
  auto stream = space->acquire_stream();
  ArrowDeviceArray host{};
  host.array = *array;
  host.device_id = -1;
  host.device_type = ARROW_DEVICE_CPU;
  auto table = cudf::from_arrow_host(schema, &host, stream);
  stream.sync();
  impl_->fragment->push_input_batch(stream_id, sirius::make_data_batch(std::move(table), *space, stream, {}));
  ++impl_->arrow_batch_index;
  const char *profile = std::getenv("SEEKDB_SIRIUS_PROFILE");
  if (profile && std::string(profile) == "1") {
    std::fprintf(stderr, "sirius arrow placement: device=%d rows=%lld bytes=%llu\n",
        space->get_device_id(), static_cast<long long>(array->length),
        static_cast<unsigned long long>(decoded_bytes));
  }
}
