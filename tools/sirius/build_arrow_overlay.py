#!/usr/bin/env python3
"""Build a pinned Sirius Arrow-input overlay without modifying the source/build tree.

Reuses an existing matching Ninja build's objects and compiler invocation; only
sirius_ffi.cpp is recompiled and the shared library is relinked to a NEW directory.
The generated overlay contains the modified sources and an auditable patch.
"""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

REVISION = "3fcff6a0488516db09ddc7e993564eb9a98b72d3"
LIB_SHA256 = "cf5c490ac32f3cf7169bea8011688eedaefb634ee53741a3f475b7d729bd78ae"


def build(source, build_dir, output, patch_only=False):
    source, build_dir = Path(source).resolve(), Path(build_dir).resolve()
    output = Path(output).resolve()
    if output == source or source in output.parents or output == build_dir or build_dir in output.parents:
        raise ValueError("Overlay output must be outside the original Sirius source/build tree")
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if revision != REVISION:
        raise ValueError(f"This prototype requires Sirius {REVISION}")
    if subprocess.check_output(["git", "-C", str(source), "diff", "HEAD", "--", "src", "include"]):
        raise ValueError("Sirius source has local edits; refusing to reuse mismatched objects")
    output.mkdir(parents=True, exist_ok=True)
    edits = {}
    header = "include/sirius/ffi.hpp"
    original = (source / header).read_text()
    marker = "  std::size_t relay_from(Fragment& source,"
    assert original.count(marker) == 1
    edits[header] = original.replace(marker,
        "  // Borrowed Arrow C schema/array; synchronized GPU copy, BIGINT/DOUBLE only.\n"
        "  void push_arrow(std::uint64_t stream_id, std::uintptr_t schema, std::uintptr_t array);\n\n" + marker)
    header = "src/exec/streaming_fragment.hpp"
    original = (source / header).read_text()
    marker = "  void close_input(stream_id_t id, sender_id_t sender);"
    assert original.count(marker) == 1
    edits[header] = original.replace(marker, marker + """

  // Internal host-ingestion hook. Caller validates schema and serializes access.
  void push_input_batch(stream_id_t id, std::shared_ptr<cucascade::data_batch> batch) {
    require_built("push_input_batch");
    if (_phase.load() != phase::built || !_session.push(id, std::move(batch))) {
      throw std::runtime_error("push_input_batch: fragment/input is closed or already running");
    }
  }
""")
    original = (source / "src/sirius_ffi.cpp").read_text()
    includes = '#include <arrow/c/abi.h>\n#include <cudf/interop.hpp>\n#include "data/data_batch_utils.hpp"\n'
    marker = "std::size_t Fragment::relay_from("
    assert original.count(marker) == 1
    body = (Path(__file__).parent / "arrow_input/fragment_arrow.cpp.inc").read_text()
    edits["src/sirius_ffi.cpp"] = includes + original.replace(marker, body + "\n" + marker)
    patch = []
    for relative, content in edits.items():
        dest = output / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content)
        patch.extend(difflib.unified_diff((source / relative).read_text().splitlines(True),
                     content.splitlines(True), fromfile="a/" + relative, tofile="b/" + relative))
    (output / "sirius-arrow-input.patch").write_text("".join(patch))
    if patch_only:
        print(output / "sirius-arrow-input.patch")
        return
    library = build_dir / "extension/sirius/libsirius.so"
    with library.open("rb") as built:
        if hashlib.file_digest(built, "sha256").hexdigest() != LIB_SHA256:
            raise ValueError("Existing Sirius library does not match the validated build; use --patch-only for a full rebuild")
    commands = json.loads((build_dir / "compile_commands.json").read_text())
    entry = next(c for c in commands if c["file"] == str(source / "src/sirius_ffi.cpp"))
    compile_args = shlex.split(entry["command"])
    old_object = compile_args[compile_args.index("-o") + 1]
    new_object = str(output / "sirius_ffi.cpp.o")
    compile_args[compile_args.index("-o") + 1] = new_object
    compile_args[compile_args.index(str(source / "src/sirius_ffi.cpp"))] = str(output / "src/sirius_ffi.cpp")
    compile_args[1:1] = ["-I" + str(output / "include"), "-I" + str(output / "src")]
    (output / "compile-command.json").write_text(json.dumps(compile_args, indent=2))
    subprocess.run(compile_args, cwd=entry["directory"], check=True)
    target = "extension/sirius/libsirius.so.0.0.0"
    commands = subprocess.check_output(["ninja", "-t", "commands", target], cwd=build_dir, text=True)
    link = shlex.split(commands.strip().splitlines()[-1])
    # Ninja's CMake link rule is ': && compiler ... && :'. Run only the compiler.
    if link[:2] != [":", "&&"] or link[-2:] != ["&&", ":"]:
        raise ValueError("Unrecognized link rule; refusing to run it")
    link = link[2:-2]
    if old_object not in link:
        raise ValueError("FFI object missing from link rule")
    link[link.index(old_object)] = new_object
    link[link.index("-o") + 1] = str(output / "libsirius.so.0.0.0")
    link = ["-Wl,--dependency-file=" + str(output / "link.d")
            if arg.startswith("-Wl,--dependency-file=") else arg for arg in link]
    (output / "link-command.json").write_text(json.dumps(link, indent=2))
    subprocess.run(link, cwd=build_dir, check=True)
    for name in ("libsirius.so", "libsirius.so.0"):
        path = output / name
        if path.is_symlink():
            path.unlink()
        path.symlink_to("libsirius.so.0.0.0")
    print(output / "libsirius.so")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source")
    p.add_argument("build")
    p.add_argument("output")
    p.add_argument("--patch-only", action="store_true", help="Emit sources/patch without requiring existing build artifacts")
    args = p.parse_args()
    build(args.source, args.build, args.output, args.patch_only)
