// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "arrow_json.h"
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

namespace seekdb_sirius {
namespace {
template <class T> struct ReleaseGuard {
  T &value;
  ~ReleaseGuard() { if (value.release) { value.release(&value); } }
};

class Writer {
public:
  Writer(char *data, size_t capacity) : data_(data), capacity_(capacity) {}
  void add(std::string_view value) {
    if (value.size() > capacity_ - size_) { throw ResultTooLarge(); }
    std::memcpy(data_ + size_, value.data(), value.size());
    size_ += value.size();
  }
  void quoted(std::string_view value) {
    add("\"");
    for (unsigned char c : value) {
      if (c == '"' || c == '\\') {
        char escaped[2] = {'\\', static_cast<char>(c)};
        add({escaped, 2});
      } else if (c < 0x20) {
        char escaped[7];
        std::snprintf(escaped, sizeof(escaped), "\\u%04x", c);
        add({escaped, 6});
      } else {
        const char byte = static_cast<char>(c);
        add({&byte, 1});
      }
    }
    add("\"");
  }
  template <class T> void number(T value) {
    char buffer[128];
    auto converted = std::to_chars(buffer, buffer + sizeof(buffer), value);
    if (converted.ec != std::errc()) { throw std::runtime_error("Cannot encode Arrow number"); }
    add({buffer, static_cast<size_t>(converted.ptr - buffer)});
  }
  size_t size() const { return size_; }
private:
  char *data_;
  size_t capacity_;
  size_t size_ = 0;
};

void require(bool value, const char *message) {
  if (!value) { throw std::runtime_error(message); }
}

std::string stream_error(ArrowArrayStream &stream) {
  const char *message = stream.get_last_error ? stream.get_last_error(&stream) : nullptr;
  return message ? message : "Arrow stream failed";
}

int decimal_scale(const char *format) {
  int precision = 0, scale = 0, width = 128, consumed = 0;
  const int fields = std::sscanf(format, "d:%d,%d%n", &precision, &scale, &consumed);
  require(fields == 2 && precision > 0 && precision <= 38 && scale >= 0 && scale <= 38,
          "Only decimal128 with nonnegative scale up to 38 is supported");
  if (format[consumed] != '\0') {
    int tail = 0;
    require(std::sscanf(format + consumed, ",%d%n", &width, &tail) == 1 && width == 128
            && format[consumed + tail] == '\0', "Only decimal128 is supported");
  }
  return scale;
}

void check_type(const ArrowSchema &schema) {
  require(schema.format != nullptr && schema.dictionary == nullptr && schema.n_children == 0,
          "Dictionary and nested Arrow columns are not supported by the Sirius SQL prototype");
  std::string_view f(schema.format);
  if (f == "n" || f == "b" || f == "c" || f == "C" || f == "s" || f == "S"
      || f == "i" || f == "I" || f == "l" || f == "L" || f == "f" || f == "g"
      || f == "u" || f == "U") { return; }
  if (f.substr(0, 2) == "d:") { decimal_scale(schema.format); return; }
  throw std::runtime_error("Unsupported Arrow result format: " + std::string(f));
}

bool is_null(const ArrowArray &array, int64_t index) {
  require(index >= 0 && index < array.length && array.offset >= 0
          && array.offset <= INT64_MAX - index, "Invalid Arrow array offset/length");
  require(array.n_buffers >= 1 && array.buffers != nullptr, "Missing Arrow validity buffer slot");
  const auto *validity = static_cast<const uint8_t *>(array.buffers[0]);
  const int64_t pos = array.offset + index;
  return validity != nullptr && (validity[pos / 8] & (1u << (pos % 8))) == 0;
}

template <class T> T load(const ArrowArray &array, int64_t pos) {
  require(array.n_buffers >= 2 && array.buffers[1] != nullptr, "Missing Arrow values buffer");
  T value;
  std::memcpy(&value, static_cast<const char *>(array.buffers[1]) + pos * sizeof(T), sizeof(T));
  return value;
}

template <class Offset> void text_value(Writer &writer, const ArrowArray &array, int64_t pos) {
  require(array.n_buffers >= 3 && array.buffers[1] != nullptr, "Missing Arrow string offsets");
  Offset begin, end;
  const auto *offsets = static_cast<const char *>(array.buffers[1]);
  std::memcpy(&begin, offsets + pos * sizeof(Offset), sizeof(Offset));
  std::memcpy(&end, offsets + (pos + 1) * sizeof(Offset), sizeof(Offset));
  require(begin >= 0 && end >= begin && (array.buffers[2] != nullptr || end == 0), "Invalid Arrow string offsets");
  const char *data = array.buffers[2] ? static_cast<const char *>(array.buffers[2]) + begin : "";
  writer.quoted({data, static_cast<size_t>(end - begin)});
}

void decimal_value(Writer &writer, const ArrowArray &array, int64_t pos, int scale) {
  const __int128 value = load<__int128>(array, pos);
  unsigned __int128 magnitude = value < 0 ? -static_cast<unsigned __int128>(value) : value;
  char digits[40];
  size_t n = sizeof(digits);
  do {
    digits[--n] = '0' + magnitude % 10;
    magnitude /= 10;
  } while (magnitude != 0);
  std::string result(digits + n, sizeof(digits) - n);
  if (scale > 0) {
    if (result.size() <= static_cast<size_t>(scale)) { result.insert(0, scale + 1 - result.size(), '0'); }
    result.insert(result.size() - scale, ".");
  }
  if (value < 0) { result.insert(0, "-"); }
  writer.quoted(result);
}

void value(Writer &writer, const ArrowSchema &schema, const ArrowArray &array, int64_t row) {
  if (std::strcmp(schema.format, "n") == 0) { writer.add("null"); return; }
  if (is_null(array, row)) { writer.add("null"); return; }
  require(array.dictionary == nullptr && array.n_children == 0, "Unexpected dictionary/nested Arrow array");
  const int64_t pos = row + array.offset;
  switch (schema.format[0]) {
    case 'b': {
      const uint8_t byte = load<uint8_t>(array, pos / 8);
      writer.add((byte & (1u << (pos % 8))) ? "true" : "false"); break;
    }
    case 'c': writer.number(load<int8_t>(array, pos)); break;
    case 'C': writer.number(load<uint8_t>(array, pos)); break;
    case 's': writer.number(load<int16_t>(array, pos)); break;
    case 'S': writer.number(load<uint16_t>(array, pos)); break;
    case 'i': writer.number(load<int32_t>(array, pos)); break;
    case 'I': writer.number(load<uint32_t>(array, pos)); break;
    case 'l': writer.number(load<int64_t>(array, pos)); break;
    case 'L': writer.number(load<uint64_t>(array, pos)); break;
    case 'f': {
      const float v = load<float>(array, pos);
      require(std::isfinite(v), "NaN/Infinity cannot be represented in JSON"); writer.number(v); break;
    }
    case 'g': {
      const double v = load<double>(array, pos);
      require(std::isfinite(v), "NaN/Infinity cannot be represented in JSON"); writer.number(v); break;
    }
    case 'u': text_value<int32_t>(writer, array, pos); break;
    case 'U': text_value<int64_t>(writer, array, pos); break;
    case 'd': decimal_value(writer, array, pos, decimal_scale(schema.format)); break;
    default: throw std::runtime_error("Unsupported Arrow result type");
  }
}
}

size_t arrow_to_json(ArrowArrayStream &stream, char *output, size_t capacity) {
  ReleaseGuard<ArrowArrayStream> release_stream{stream};
  require(output != nullptr && stream.release && stream.get_schema && stream.get_next, "Invalid Arrow stream/output");
  ArrowSchema schema{};
  ReleaseGuard<ArrowSchema> release_schema{schema};
  if (stream.get_schema(&stream, &schema) != 0) { throw std::runtime_error(stream_error(stream)); }
  require(schema.release && schema.format && std::strcmp(schema.format, "+s") == 0
          && schema.n_children >= 0 && (schema.n_children == 0 || schema.children), "Expected Arrow record batch schema");
  Writer writer(output, capacity);
  writer.add("{\"columns\":[");
  for (int64_t i = 0; i < schema.n_children; ++i) {
    require(schema.children[i] != nullptr, "Missing Arrow column schema");
    const auto &column = *schema.children[i];
    check_type(column);
    if (i) { writer.add(","); }
    writer.add("{\"name\":"); writer.quoted(column.name ? column.name : "");
    writer.add(",\"format\":"); writer.quoted(column.format); writer.add("}");
  }
  writer.add("],\"rows\":[");
  bool first = true;
  for (;;) {
    ArrowArray batch{};
    ReleaseGuard<ArrowArray> release_batch{batch};
    if (stream.get_next(&stream, &batch) != 0) { throw std::runtime_error(stream_error(stream)); }
    if (!batch.release) { break; }
    require(batch.n_children == schema.n_children && batch.length >= 0 && batch.offset >= 0
            && batch.length <= INT64_MAX - batch.offset
            && (batch.n_children == 0 || batch.children), "Invalid Arrow record batch");
    for (int64_t row = 0; row < batch.length; ++row) {
      if (!first) { writer.add(","); } first = false;
      writer.add("[");
      for (int64_t col = 0; col < schema.n_children; ++col) {
        if (col) { writer.add(","); }
        require(batch.children[col] != nullptr, "Missing Arrow column");
        value(writer, *schema.children[col], *batch.children[col], batch.offset + row);
      }
      writer.add("]");
    }
  }
  writer.add("]}");
  return writer.size();
}
}
