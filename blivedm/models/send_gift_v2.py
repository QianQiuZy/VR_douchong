import base64
import binascii
import dataclasses
from typing import Annotated, TypedDict

import pure_protobuf.annotations as pb_anno
import pure_protobuf.message as pb_msg

from . import web


class SendGiftV2Command(TypedDict):
    pb: str


class SendGiftV2DecodeError(ValueError):
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class MedalInfo(pb_msg.BaseMessage):
    anchor_uid: Annotated[int, pb_anno.Field(1)] = 0
    medal_level: Annotated[int, pb_anno.Field(5)] = 0
    medal_name: Annotated[str, pb_anno.Field(6)] = ''
    guard_level: Annotated[int, pb_anno.Field(11)] = 0


@dataclasses.dataclass(frozen=True, slots=True)
class BlindGift(pb_msg.BaseMessage):
    gift_action: Annotated[int, pb_anno.Field(1)] = 0
    original_gift_id: Annotated[int, pb_anno.Field(2)] = 0
    original_gift_name: Annotated[str, pb_anno.Field(3)] = ''
    action: Annotated[str, pb_anno.Field(5)] = ''
    blind_price: Annotated[int, pb_anno.Field(6)] = 0


@dataclasses.dataclass(frozen=True, slots=True)
class GiftEffect(pb_msg.BaseMessage):
    img_basic: Annotated[str, pb_anno.Field(1)] = ''


@dataclasses.dataclass(frozen=True, slots=True)
class GiftData(pb_msg.BaseMessage):
    gift_id: Annotated[int, pb_anno.Field(1)] = 0
    gift_name: Annotated[str, pb_anno.Field(2)] = ''
    num: Annotated[int, pb_anno.Field(3)] = 0
    gift_type: Annotated[int, pb_anno.Field(4)] = 0
    price: Annotated[int, pb_anno.Field(5)] = 0
    total_coin: Annotated[int, pb_anno.Field(6)] = 0
    discount_price: Annotated[int, pb_anno.Field(7)] = 0
    coin_type: Annotated[str, pb_anno.Field(8)] = ''
    tid: Annotated[str, pb_anno.Field(9)] = ''
    timestamp: Annotated[int, pb_anno.Field(10)] = 0
    rnd: Annotated[str, pb_anno.Field(12)] = ''
    action: Annotated[str, pb_anno.Field(18)] = ''
    effect: Annotated[GiftEffect, pb_anno.Field(35)] = dataclasses.field(
        default_factory=GiftEffect
    )


@dataclasses.dataclass(frozen=True, slots=True)
class SendGiftV2(pb_msg.BaseMessage):
    uid: Annotated[int, pb_anno.Field(1)] = 0
    uname: Annotated[str, pb_anno.Field(2)] = ''
    face: Annotated[str, pb_anno.Field(3)] = ''
    medal: Annotated[MedalInfo, pb_anno.Field(8)] = dataclasses.field(
        default_factory=MedalInfo
    )
    blind: Annotated[BlindGift, pb_anno.Field(9)] = dataclasses.field(
        default_factory=BlindGift
    )
    gift: Annotated[GiftData, pb_anno.Field(10)] = dataclasses.field(
        default_factory=GiftData
    )


class SendGiftV2Message:
    @classmethod
    def from_command(cls, data: SendGiftV2Command) -> web.GiftMessage:
        try:
            proto = SendGiftV2.loads(base64.b64decode(data['pb'], validate=True))
        except (binascii.Error, EOFError, KeyError, TypeError, ValueError) as exc:
            raise SendGiftV2DecodeError from exc

        gift = proto.gift
        blind = proto.blind
        if gift.gift_id <= 0 or gift.num <= 0 or not gift.gift_name:
            raise SendGiftV2DecodeError
        is_blind_box = bool(blind.original_gift_id or blind.original_gift_name)
        total_price = gift.price * gift.num if is_blind_box else gift.total_coin

        return web.GiftMessage(
            gift_name=gift.gift_name,
            num=gift.num,
            uname=proto.uname,
            face=proto.face,
            guard_level=proto.medal.guard_level,
            uid=proto.uid,
            timestamp=gift.timestamp,
            gift_id=gift.gift_id,
            gift_type=gift.gift_type,
            gift_img_basic=gift.effect.img_basic,
            action=gift.action,
            price=gift.price,
            rnd=gift.rnd,
            coin_type=gift.coin_type,
            total_coin=gift.total_coin,
            total_price=total_price,
            tid=gift.tid,
            medal_level=proto.medal.medal_level,
            medal_name=proto.medal.medal_name,
            medal_ruid=proto.medal.anchor_uid,
        )
