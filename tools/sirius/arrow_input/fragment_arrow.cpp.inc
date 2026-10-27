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
  const auto decoded_bytes = seekdb_sirius::arrow_batch_bytes(*schema, *array);
  for (int64_t i = 0; i < schema->n_children; ++i) {
    const auto *column = schema->children[i];
    if (seekdb_sirius::arrow_type(*column).sql != it->second.type_names[i]
        || column->name != it->second.names[i]) {
      throw std::invalid_argument("push_arrow: columns must match declared names and SQL types");
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
  auto columns = table->release();
  for (int64_t i = 0; i < schema->n_children; ++i) {
    const auto type = seekdb_sirius::arrow_type(*schema->children[i]);
    if (type.precision) {
      const auto id = type.precision <= 9 ? cudf::type_id::DECIMAL32
                    : type.precision <= 18 ? cudf::type_id::DECIMAL64 : cudf::type_id::DECIMAL128;
      const cudf::data_type target_type(id, -type.scale);
      if (columns[i]->type() != target_type) {
        columns[i] = cudf::cast(columns[i]->view(), target_type, stream);
      }
    }
  }
  table = std::make_unique<cudf::table>(std::move(columns));
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
