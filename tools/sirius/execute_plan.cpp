// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_client.h"
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

int main(int argc, char **argv)
{
  if (argc != 2) { std::cerr << "Usage: sirius_execute_plan PLAN.pb\n"; return 2; }
  std::ifstream input(argv[1], std::ios::binary | std::ios::ate);
  if (!input || input.tellg() <= 0 || input.tellg() > 1024 * 1024) {
    std::cerr << "Expected a readable, nonempty plan of at most 1 MiB\n"; return 2;
  }
  input.seekg(0);
  std::vector<uint8_t> plan((std::istreambuf_iterator<char>(input)), {});
  std::vector<char> output(1024 * 1024);
  oceanbase::sql::SiriusClient client;
  char error[512]{};
  size_t size = 0;
  int status = client.execute(plan.data(), plan.size(), output.data(), output.size(), &size, error, sizeof(error));
  if (status) { std::cerr << error << '\n'; return status; }
  std::cout.write(output.data(), size); std::cout << '\n';
}
