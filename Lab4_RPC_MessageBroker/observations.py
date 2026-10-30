"""Optional receiver-side evidence for schema experiments; no work on import."""
from google.protobuf.json_format import MessageToDict


def field_state(message, name):
    if name not in message.DESCRIPTOR.fields_by_name:
        return dict(known=False, present=None, value=None)
    return dict(known=True, present=message.HasField(name), value=getattr(message, name))


def snapshot(message):
    fields = message.DESCRIPTOR.fields_by_name
    return dict(
        message_type=message.DESCRIPTOR.full_name,
        body=MessageToDict(message, preserving_proto_field_name=True),
        comment=field_state(message, "comment"),
        promo_code=field_state(message, "promo_code"),
        prices_minor=[item.unit_price_minor for item in message.items] if "items" in fields else [],
    )
