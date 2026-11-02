// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <array>

namespace oceanbase { namespace sql {
inline void sirius_append_le(std::string &out, uint64_t value, unsigned width) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  out.append(reinterpret_cast<const char *>(&value), width);
#else
  for (unsigned i = 0; i < width; ++i) out.push_back(static_cast<char>(value >> (8 * i)));
#endif
}

inline void sirius_append_decimal_storage(std::string &out, const void *data, unsigned width) {
  __int128 value = 0;
  if (width == 4) { int32_t v; std::memcpy(&v, data, 4); value = v; }
  else if (width == 8) { int64_t v; std::memcpy(&v, data, 8); value = v; }
  else if (width == 16) std::memcpy(&value, data, 16);
  else throw std::invalid_argument("Invalid Sirius decimal integer width");
  const auto bits = static_cast<unsigned __int128>(value);
  sirius_append_le(out, static_cast<uint64_t>(bits), 8);
  sirius_append_le(out, static_cast<uint64_t>(bits >> 64), 8);
}

// ObNumber's base-1e9 digits, most significant first. exponent is the
// decoded exponent of the first digit, not a decimal/scientific exponent.
inline void sirius_append_number_digits(std::string &out, const uint32_t *digits,
                                        unsigned count, int exponent, bool negative,
                                        unsigned precision, unsigned scale) {
  static const auto powers = [] {
    std::array<unsigned __int128, 39> p{}; p[0] = 1;
    for (size_t i = 1; i < p.size(); ++i) p[i] = p[i-1] * 10;
    return p;
  }();
  if (!precision || precision > 38 || scale > precision || (count && !digits))
    throw std::invalid_argument("Invalid Sirius number shape");
  unsigned __int128 value = 0;
  const auto bound = powers[precision];
  for (unsigned i = 0; i < count; ++i) {
    if (digits[i] >= 1000000000) throw std::invalid_argument("Invalid Sirius number digit");
    if (!digits[i]) continue;
    const int power = 9 * (exponent - static_cast<int>(i)) + static_cast<int>(scale);
    unsigned __int128 term;
    if (power >= 0) {
      if (power > 38 || digits[i] > (bound - 1 - value) / powers[power])
        throw std::overflow_error("Sirius number precision exceeded");
      term = digits[i] * powers[power];
    } else {
      if (power < -9 || digits[i] % static_cast<uint64_t>(powers[-power]))
        throw std::invalid_argument("Sirius number scale mismatch");
      term = digits[i] / static_cast<uint64_t>(powers[-power]);
      if (term >= bound - value) throw std::overflow_error("Sirius number precision exceeded");
    }
    value += term;
  }
  if (negative) value = 0 - value;
  sirius_append_le(out, static_cast<uint64_t>(value), 8);
  sirius_append_le(out, static_cast<uint64_t>(value >> 64), 8);
}

// Exact scaled decimal128; the textual ObNumber representation is converted
// once in seekdb, never parsed as floating point or decoded per cell in Python.
inline void sirius_append_decimal(std::string &out, const char *text, size_t length,
                                  unsigned precision, unsigned scale) {
  if (!length || !precision || precision > 38 || scale > precision)
    throw std::invalid_argument("Invalid Sirius decimal shape");
  size_t pos = 0; bool negative = text[0] == '-';
  if (negative || text[0] == '+') ++pos;
  bool dot = false, digit = false; unsigned fractional = 0;
  unsigned __int128 value = 0, bound = 1;
  for (unsigned i = 0; i < precision; ++i) bound *= 10;
  for (; pos < length; ++pos) {
    const char c = text[pos];
    if (c == '.' && !dot) { dot = true; continue; }
    if (c < '0' || c > '9') throw std::invalid_argument("Invalid Sirius decimal value");
    digit = true;
    if (dot && fractional >= scale) {
      if (c != '0') throw std::invalid_argument("Sirius decimal scale mismatch");
      continue;
    }
    if (value > (bound - 1 - (c - '0')) / 10)
      throw std::overflow_error("Sirius decimal precision exceeded");
    value = value * 10 + (c - '0');
    if (dot) ++fractional;
  }
  if (!digit) throw std::invalid_argument("Empty Sirius decimal value");
  while (fractional++ < scale) {
    if (value > (bound - 1) / 10) throw std::overflow_error("Sirius decimal precision exceeded");
    value *= 10;
  }
  if (negative) value = 0 - value;
  sirius_append_le(out, static_cast<uint64_t>(value), 8);
  sirius_append_le(out, static_cast<uint64_t>(value >> 64), 8);
}

struct SiriusBinaryColumn {
  std::string validity, offsets, values;
  unsigned width;
  bool variable;
  explicit SiriusBinaryColumn(unsigned w, bool v) : width(w), variable(v) { reset(); }
  void reset() {
    validity.clear(); offsets.clear(); values.clear();
    if (variable) sirius_append_le(offsets, 0, 4);
  }
  void mark(size_t row, bool valid) {
    if ((row % 8) == 0) validity.push_back(0);
    if (valid) validity.back() = static_cast<char>(static_cast<unsigned char>(validity.back()) | (1u << (row % 8)));
  }
  void finish(bool is_null) {
    if (is_null && !variable) values.append(width, '\0');
    if (variable) {
      if (values.size() > 16 * 1024 * 1024) throw std::length_error("Sirius string batch exceeds limit");
      sirius_append_le(offsets, values.size(), 4);
    }
  }
  size_t size() const { return validity.size() + offsets.size() + values.size() + 12; }
};
} }
