"""Generate Python messages and gRPC bindings for each schema version."""

from pathlib import Path
import sys

import grpc_tools
from grpc_tools import protoc


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    well_known = Path(grpc_tools.__file__).parent / "_proto"
    for version in ("v1", "v2", "v2_breaking"):
        generated = root / "server" / "generated" / version
        generated.mkdir(parents=True, exist_ok=True)
        # Virtual include paths keep generated imports package-qualified.
        result = protoc.main([
            "grpc_tools.protoc",
            f"-Iserver/generated/{version}={root / 'schemas' / version}",
            f"-I{well_known}",
            f"--python_out={root}",
            f"--pyi_out={root}",
            f"--grpc_python_out={root}",
            f"--descriptor_set_out={generated / 'orders.desc'}",
            "--include_imports",
            str(root / "schemas" / version / "orders.proto"),
        ])
        if result:
            return result
        print(f"Generated Python bindings: {generated}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
