"""Select one generated schema per process; keep the wire package stable."""
from importlib import import_module

VERSIONS = ("v1", "v2", "v2_breaking")


def load_schema(version="v1"):
    if version not in VERSIONS:
        raise ValueError(f"Unknown schema: {version}")
    return (import_module(f"server.generated.{version}.orders_pb2"),
            import_module(f"server.generated.{version}.orders_pb2_grpc"))
