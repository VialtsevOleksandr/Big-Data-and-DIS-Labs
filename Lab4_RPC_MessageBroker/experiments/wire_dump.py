"""Explain field tags in one real serialized order; all offsets are byte offsets."""
from google.protobuf.descriptor import FieldDescriptor as F


def read_varint(data, offset):
    value = 0
    shift = 0
    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if byte < 128:
            return value, offset
        shift += 7
    raise ValueError("Truncated or invalid varint")


def describe(data, descriptor, base=0, indent=""):
    lines = []
    offset = 0
    while offset < len(data):
        start = offset
        tag, offset = read_varint(data, offset)
        number, wire = tag >> 3, tag & 7
        field = descriptor.fields_by_number.get(number)
        name = field.name if field else "unknown"
        tag_hex = data[start:offset].hex(" ").upper()
        prefix = f"{indent}{base + start:04X}: tag=[{tag_hex}] field={number} ({name}) wire={wire}"
        if wire == 0:
            value_start = offset
            value, offset = read_varint(data, offset)
            enum = ""
            if field and field.enum_type and value in field.enum_type.values_by_number:
                enum = " " + field.enum_type.values_by_number[value].name
            lines.append(f"{prefix} value=[{data[value_start:offset].hex(' ').upper()}] -> {value}{enum}")
        elif wire == 2:
            length_start = offset
            length, offset = read_varint(data, offset)
            payload_start = offset
            offset += length
            if offset > len(data):
                raise ValueError("Truncated length-delimited field")
            payload = data[payload_start:offset]
            suffix = ""
            if field and field.type == F.TYPE_STRING:
                suffix = " -> " + repr(payload.decode("utf-8"))
            lines.append(f"{prefix} length=[{data[length_start:payload_start].hex(' ').upper()}] -> {length} bytes{suffix}")
            if field and field.message_type:
                lines.extend(describe(payload, field.message_type, base + payload_start, indent + "  "))
        elif wire in (1, 5):
            length = 8 if wire == 1 else 4
            lines.append(f"{prefix} value=[{data[offset:offset + length].hex(' ').upper()}]")
            offset += length
        else:
            raise ValueError(f"Unsupported wire type: {wire}")
    return lines


def dump(message):
    payload = message.SerializeToString()
    lines = [f"Order: {message.order_id}", f"Size: {len(payload)} bytes", "", "HEX:"]
    for offset in range(0, len(payload), 16):
        lines.append(f"{offset:04X}: {payload[offset:offset + 16].hex(' ').upper()}")
    lines.extend(["", "FIELDS (offsets and bytes are hexadecimal):", *describe(payload, message.DESCRIPTOR)])
    return "\n".join(lines) + "\n"
