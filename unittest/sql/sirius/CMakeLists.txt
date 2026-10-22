find_package(GTest REQUIRED)
add_library(fake_sirius_bridge SHARED fake_bridge.cpp)
target_include_directories(fake_sirius_bridge PRIVATE "${SEEKDB_SIRIUS_API_DIR}")
add_library(wrong_sirius_bridge SHARED fake_bridge.cpp)
target_include_directories(wrong_sirius_bridge PRIVATE "${SEEKDB_SIRIUS_API_DIR}")
target_compile_definitions(wrong_sirius_bridge PRIVATE WRONG_ABI=1)
add_executable(test_sirius_bridge test_sirius_bridge.cpp)
target_link_libraries(test_sirius_bridge PRIVATE sirius_client sirius_arrow_json GTest::gtest_main)
target_compile_definitions(test_sirius_bridge PRIVATE
  FAKE_BRIDGE_PATH="$<TARGET_FILE:fake_sirius_bridge>"
  WRONG_BRIDGE_PATH="$<TARGET_FILE:wrong_sirius_bridge>")
add_dependencies(test_sirius_bridge fake_sirius_bridge wrong_sirius_bridge)
include(GoogleTest)
gtest_discover_tests(test_sirius_bridge)
