"""One schema per short-lived Python process to avoid descriptor-name collisions."""
import argparse
import json
from pathlib import Path

from google.protobuf.json_format import MessageToDict, ParseDict

from observations import snapshot
from schema import load_schema, VERSIONS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", choices=VERSIONS, required=True)
    parser.add_argument("--operation", choices=("encode", "inspect", "relay"), required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output")
    parser.add_argument("--mode", choices=("direct", "json", "fields"), default="direct")
    args = parser.parse_args()
    pb, _ = load_schema(args.schema)
    if args.operation == "encode":
        message = ParseDict(json.loads(Path(args.input).read_text(encoding="utf-8")), pb.Order())
    else:
        message = pb.Order.FromString(Path(args.input).read_bytes())
    before = snapshot(message)
    if args.operation == "relay":
        if args.mode == "json":
            message = ParseDict(MessageToDict(message, preserving_proto_field_name=True), pb.Order())
        elif args.mode == "fields":
            source = message
            message = pb.Order(order_id=source.order_id, buyer_id=source.buyer_id, status=source.status,
                               created_at=source.created_at, shipping_address=source.shipping_address, items=source.items)
            for name in ("comment", "promo_code"):
                if name in source.DESCRIPTOR.fields_by_name and source.HasField(name):
                    setattr(message, name, getattr(source, name))
    if args.output:
        Path(args.output).write_bytes(message.SerializeToString())
    elif args.operation != "inspect":
        parser.error("--output is required for encode/relay")
    print(json.dumps(dict(language="Python", schema=args.schema, operation=args.operation, mode=args.mode,
                          before=before, after=snapshot(message)), ensure_ascii=False))


if __name__ == "__main__":
    main()
